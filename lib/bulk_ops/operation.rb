require 'bulk_ops/verification'
module BulkOps
  class Operation < ActiveRecord::Base
    self.table_name = "bulk_ops_operations"
    belongs_to :user
    has_many :work_proxies, class_name: "::BulkOps::WorkProxy"

    include BulkOps::Verification

    attr_accessor :work_type, :visibility, :reference_identifier, :metadata
    
    delegate  :can_merge?, :merge_pull_request, to: :git

    def self.unique_name name, user
      while  BulkOps::Operation.find_by(name: name) || BulkOps::GithubAccess.list_branch_names(user).include?(name) do
        if ['-','_'].include?(name[-2]) && name[-1].to_i > 0
          name = name[0..-2]+(name[-1].to_i + 1).to_s
        else
          name = name + "_1"
        end
      end
      return name
    end

    def proxy_errors
      work_proxies.reduce([]) do |errors, proxy| 
        if proxy.proxy_errors
          errors += proxy.proxy_errors
        elsif proxy.status == "job_error"
          errors += BulkOps::Error.new(type: :job_failure, object_id: proxy.work_id, message: proxy.message)
        end
      end
    end

    def proxy_states
      states = {}
      work_proxies.each{|proxy| (states[proxy.status] ||= []) << proxy }
      states
    end

    def self.schema
      ScoobySnacks::METADATA_SCHEMA
    end

    def schema
      self.class.schema
    end

    def work_type
      options["work_type"] || "work"
    end
    
    def reference_identifier
      options["reference_identifier"] || "id/row"
    end
    
    def set_stage new_stage
      update(stage: new_stage)
    end

    def destroy_all_works_and_proxies
      work_proxies.each do |proxy| 
        if BulkOps::SolrService.record_exists?(proxy.work_id)
          ActiveFedora::Base.find(work_id).destroy
        end
        proxy.destroy
      end
      update(stage: "waiting",
             status: "reverted changes")
  
   end

    def destroy_all_works
      work_proxies.each do |proxy| 
        if BulkOps::SolrService.record_exists?(proxy.work_id)
          ActiveFedora::Base.find(work_id).destroy
        end
        proxy.update(status: "destroyed", message: "The work created by this proxy was destroyed by the user")
      end
      update(stage: "waiting",
             status: "reverted changes")
    end

    def destroy_all_proxies
      work_proxies.each do |proxy| 
        proxy.destroy
      end
      update(stage: "waiting",
             status: "reverted changes")      
    end

    def apply!
      update({stage: "running", 
              status: "OK",
              message: "Bulk operation initiated by #{user.name || user.email}"})
      # We should now on the master branch. Make sure the correct spreadsheet version is loaded
      final_spreadsheet

      # In case this operation has run before, gather all work proxies that are completed and exclude them from the application
      complete_proxies = work_proxies.select{|proxy| proxy.status == "complete" && proxy.work_id.present?}
      incomplete_row_numbers = Array(0..@metadata.length-1) - complete_proxies.map(&:row_number)

      # Destroy all proxies corresponding to incomplete rows
      (work_proxies - complete_proxies).each{|proxy| proxy.destroy!}

      # Create a new work proxy for each incomplete row
      # All the proxies need to exist before parsing in order to correctly recognize relationships
      incomplete_row_numbers.each do |row_number|
        values = @metadata[row_number]
        next if values.to_s.gsub(',','').blank?
        next if BulkOps::Parser.is_file_set? @metadata, row_number
        work_proxies.create(status: "new",
                            last_event: DateTime.now,
                            work_type: work_type,
                            row_number: row_number,
                            visibility: options['visibility'],
                            message: "created during ingest initiated by #{user.name || user.email}")
 
      end
      # Reload the operation so that it can recognize its new proxies
      reload
      # Parse each spreadsheet row and create a background job for each proxy we just created
      incomplete_row_numbers.each do |row_number|
        values = @metadata[row_number]
        proxy = work_proxies.find_by(row_number: row_number)
        proxy.update(message: "interpreted at #{DateTime.now.strftime("%d/%m/%Y %H:%M")} " + proxy.message)
        data = BulkOps::Parser.new(proxy, @metadata,options).interpret_data(raw_row: values)
        next unless proxy.proxy_errors.blank?
        BulkOps::WorkJob.perform_later(proxy.work_type || "Work",
                                             user.email,
                                             data,
                                             proxy.id,
                                             proxy.visibility)
      end
      # If any errors have occurred, make sure they are logged in github and users are notified.
      report_errors!
    end

    def check_if_finished
      return unless stage == "running" && !busy?

      update(stage: "finishing")

      # Attempt to resolve each dangling (objectless) relationships
      relationships = work_proxies.reduce([]){|all_rels,proxy| all_rels + proxy.relationships.select{|rel| rel.status == "pending"}}
      relationships.each do |rel| 
        begin
          rel.resolve! 
        rescue StandardError => e
          @operation_errors << BulkOps::Error.new(:relationship_error, row_number: proxy.row_number, object_id: relationship.id, message: "#{e.class} - #{e.message}" )
        end
      end
      
      work_proxies.each do |proxy| 
        work = nil
        begin
          work = Work.find(proxy.work_id).save
        rescue StandardError => e
          @operation_errors << BulkOps::Error.new(:ingest_failure, row_number: proxy.row_number, object_id: proxy.id, message: "#{e.class} - #{e.message}")
        end
      end

      new_stage = accumulated_errors.blank? ? "complete" : "errors"
      update(stage: new_stage)
      report_errors!
      lift_holds
    end

    def lift_holds
      work_proxies.each { |proxy| proxy.lift_hold}
    end

    def place_holds
      work_proxies.each { |proxy| proxy.place_hold}
    end

    def apply_update! spreadsheet

      # this array will keep track of any current proxies not included in the final spreadsheet
      abandoned_proxies = work_proxies.dup
      # Loop through the final spreadsheet
      final_spreadsheet.each_with_index do |values,row_number|     
        # Grab the work id
        work_id = false
        values.each{|field,val| work_id = val if ["id","workid","recordid"].include?(field.downcase.gsub(/-_\s/,''))}
        @operation_errors << BulkOps::Error.new(:no_work_id_field) unless work_id

        #proxy = BulkOps::WorkProxy.find_by(operation_id: id, work_id: values["work_id"])
        if (proxy = work_proxies.find_by(work_id: work_id))
          abandoned_proxies.delete(proxy)
          proxy.update(status: "updating",
                       row_number: row_number,
                       message: "update initiated by #{user.name || user.email}")
        else
          # Create a proxy for a work that is in the spreadsheet, but wasn't in the initial draft
          work_proxies.create(status: "queued",
                              last_event: DateTime.now,
                              row_number: row_number,
                              message: "created during update application, which was initiated by #{user.name || user.email}")
        end
      end

      # Loop through any proxies in the draft that were dropped from the spreadsheet
      abandoned_proxies.each do |dead_proxy|
        dead_proxy.lift_hold
        dead_proxy.destroy!
      end
      
      #loop through the work proxies to create a job for each work
      work_proxies.each do |proxy|
        data = BulkOps::Parser.new(proxy,final_spreadsheet).interpret_data(raw_row: final_spreadsheet[proxy.row_number])
        BulkOps::UpdateWorkJob.perform_later(proxy.work_type || "",
                                             user.email,
                                             data,
                                             proxy.id,
                                             proxy.visibility)
      end
      report_errors! 
    end

    def accumulated_errors
      proxy_errors + (@operation_errors || [])
      # TODO - make sure this captures all operation errors
    end

    def report_errors!
      error_file_name = BulkOps::Error.write_errors!(accumulated_errors, git)
      notify!(subject: "Errors initializing bulk operation in Hycruz", message: "Hycruz encountered some errors while it  was setting up your operation and preparing to begin. For most types of errors, the individual rows of the spreadsheet with errors will be ignored and the rest will proceed. Please consult the operation summary for real time information on the status of the operation. Details about these initialization errors can be seen on Github at the following url: https://github.com/#{git.repo}/blob/#{git.name}/#{git.name}/errors/#{error_file_name}") if error_file_name
    end

    def create_pull_request message: false
      return false unless (pull_num = git.create_pull_request(message: message))
      update(pull_id: pull_num)
      return pull_num
    end

    def finalize_draft(fields: nil, work_ids: nil)
      create_new_spreadsheet(fields: fields, work_ids: work_ids)
      update(stage: "pending")
    end

    def create_branch(fields: nil, work_ids: nil, options: nil)
      git.create_branch!
      bulk_ops_dir = Gem::Specification.find_by_name("bulk_ops").gem_dir

      #copy template files
      Dir["#{bulk_ops_dir}/#{BulkOps::TEMPLATE_DIR}/*"].each do |file| 
        git.add_file file 
      end

      #update configuration options 
      unless options.blank?
        full_options = YAML.load_file(File.join(bulk_ops_dir,BulkOps::TEMPLATE_DIR, BulkOps::OPTIONS_FILENAME))

        options.each { |option, value| full_options[option] = value }

        full_options[name] = name
        full_options[status] = status

        git.update_options full_options
      end

      create_new_spreadsheet(fields: fields, work_ids: work_ids) 
    end

    def get_spreadsheet return_headers: false
      branch = ((running? || complete?) ? "master" : nil)
      git.load_metadata return_headers: return_headers, branch: branch
    end

    def spreadsheet_count
      git.spreadsheet_count
    end

    def final_spreadsheet
      @metadata ||= git.load_metadata branch: "master"
    end

    def update_spreadsheet file, message: nil
      git.update_spreadsheet(file, message: message)
    end

    def update_options options, message=nil
      git.update_options(options, message: message)
    end

    def metadata
      @metadata ||= git.load_metadata
    end

    def options
      return {} if name.nil?
      return @options if @options
      branch = (running? || complete?) ? "master" : nil
      @options ||= git.load_options(branch: branch)
    end

    def draft?
      return (stage == 'draft')
    end

    def running?
      return (['running','finishing'].include?(stage))
    end

    def complete?
      return (stage == 'complete')
    end

    def busy?
      return true if work_proxies.any?{|prx| prx.status.downcase == "running"}
      return true if work_proxies.any?{|prx| prx.status.downcase == "queued"}
      return true if work_proxies.any?{|prx| prx.status.downcase == "starting"}
      return false
    end

    def delete_branch
      git.delete_branch!
    end

    def destroy
      git.delete_branch!
      super
    end

    def self.default_metadata_fields(labels = true)
      #returns full set of metadata parameters from ScoobySnacks to include in ingest template spreadsheet    
      field_names = []
      schema.all_fields.each do |field|
        field_names << field.name
        field_names << "#{field.name} Label" if labels && field.controlled?
      end
      return field_names
    end

    def ignored_fields
      (options['ignored headers'] || []) + BulkOps::IGNORED_COLUMNS
    end


    def error_url
      "https://github.com/#{git.repo}/tree/#{git.name}/#{git.name}/errors"
    end

    def filename_prefix
      @filename_prefix ||= options['filename_prefix']
    end

    private

    def git
      @git ||= BulkOps::GithubAccess.new(name, @user)
    end

    def create_new_spreadsheet(fields: nil, work_ids: nil)
      work_ids ||= work_proxies.map{|proxy| proxy.work_id}
      fields ||= self.class.default_metadata_fields
      work_ids = [] if work_ids.nil?
      if work_ids.count < 50
        BulkOps::CreateSpreadsheetJob.perform_now(git.name, work_ids, fields, user)
      else
        BulkOps::CreateSpreadsheetJob.perform_later(git.name, work_ids, fields, user)
      end
    end

  end
end
