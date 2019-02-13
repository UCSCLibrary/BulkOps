require 'bulk_ops/verification'
module BulkOps
  class Operation < ActiveRecord::Base
    self.table_name = "bulk_ops_operations"
    belongs_to :user
    has_many :work_proxies, class_name: "BulkOps::WorkProxy"

    include BulkOps::Verification

    attr_accessor :work_type, :visibility, :reference_identifier
    
    delegate  :can_merge?, :merge_pull_request, to: :git

    INGEST_MEDIA_PATH = "/dams_ingest"
    TEMPLATE_DIR = "lib/bulk_ops/templates"
    RELATIONSHIP_COLUMNS = ["parent","child","next"]
    SPECIAL_COLUMNS = ["parent",
                       "child",
                       "order",
                       "next",
                       "work_type",
                       "collection", 
                       "collection_title",
                       "collection_id",
                       "visibility",
                       "relationship_identifier_type",
                       "id",
                       "filename",
                       "file"]
    IGNORED_COLUMNS = ["ignore","offline_notes"]
    OPTION_REQUIREMENTS = {type: {required: true, 
                                  values:[:ingest,:update]},
                           file_method: {required: :true,
                                           values: [:replace_some,:add_remove,:replace_all]},
                           notifications: {required: true}}

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

    def type
      operation_type
    end

    def schema
      ScoobySnacks::METADATA_SCHEMA["work_types"][work_type.downcase]
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

    def apply!
      status = "#{type}ing"
      update({stage: running, message: "#{type.titleize} initiated by #{user.name || user.email}"})
#      @stage = "running"
      final_spreadsheet
 
# This commented line currently fails because it doesn't pull from the master branch by default
# It's usually already verified, but maybe we should fix this for double-checking 
# in the future
#      return unless verify

      apply_ingest! if ingest?
      apply_update! if update?
    end

    def apply_ingest! 
      #destroy any existing work proxies (which should not exist for an ingest). Create new proxies from finalized spreadsheet only.
      work_proxies.each{|proxy| proxy.destroy!}

      #create a work proxy for each row in the spreadsheet
      @metadata.each_with_index do |values,row_number|
        next if values.to_s.gsub(',','').blank?
        work_proxies.create(status: "queued",
                            last_event: DateTime.now,
                            row_number: row_number,
                            message: "created during ingest initiated by #{user.name || user.email}")
      end
      # make sure the work proxies we just created are loaded in memory
      reload
      #loop through the work proxies to create a job for each work
      work_proxies.each do |proxy|
        proxy.update(message: "interpreted at #{DateTime.now.strftime("%d/%m/%Y %H:%M")} " + proxy.message)

        data = proxy.interpret_data @metadata[proxy.row_number] 
        
        next unless proxy.proxy_errors.blank?
        
        BulkOps::CreateWorkJob.perform_later(proxy.work_type || "Work",
                                             user.email,
                                             data,
                                             proxy.id,
                                             proxy.visibility || "open")
      end
      # If any errors have occurred, make sure they are logged in github and users are notified.
      report_errors!
    end

    def delete_all
      work_proxies.each do |proxy| 
        ActiveFedora::Base.find(proxy.work_id).destroy 
        proxy.update(status: "destroyed", message: "The work created by this proxy was destroyed by the user")
      end
    end

    def check_if_finished
      return unless stage == "running" && !busy?
      update(stage: accumulated_errors.blank? ? "complete" : "errors" )
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
        data = proxy.interpret_data final_spreadsheet[proxy.row_number]
        BulkOps::UpdateWorkJob.perform_later(proxy.work_type || "",
                                             user.email,
                                             data,
                                             proxy.id,
                                             proxy.visibility || "private")
      end
      report_errors! 
    end

    def accumulated_errors
      proxy_errors + (@operation_errors || [])
      # TODO - make sure this captures all operation errors
    end

    def report_errors!
      error_file_name = BulkOps::Error.write_errors!(accumulated_errors, git)
      notify!(subject: "Errors initializing bulk #{type} in Hycruz", message: "Hycruz encountered some errors while it  was setting up your #{type} and preparing to begin. For most types of errors, the individual rows of the spreadsheet with errors will be ignored and the rest will proceed. Please consult the #{type} summary for real time information on the status of the #{type}. Details about these initialization errors can be seen on Github at the following url: https://github.com/#{git.repo}/blob/#{git.name}/#{git.name}/errors/#{error_file_name}") if error_file_name
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
      Dir["#{bulk_ops_dir}/#{TEMPLATE_DIR}/*"].each do |file| 
        git.add_file file 
      end

      #update configuration options 
      unless options.blank?
        full_options = YAML.load_file(File.join(bulk_ops_dir,TEMPLATE_DIR, BulkOps::GithubAccess::OPTIONS_FILENAME))
        options.each { |option, value| full_options[option] = value }
        git.update_options full_options
      end

      create_new_spreadsheet(fields: fields, work_ids: work_ids)
    end

    def self.works_to_csv work_ids, fields
      work_ids.reduce(fields.join(',')) do |csv, work_id| 
        if work_csv = work_to_csv(work_id,fields)
          csv + "\r\n" + work_csv
        else
          csv
        end
      end
    end

    def get_spreadsheet return_headers: false
      git.load_metadata return_headers: return_headers
    end

    def final_spreadsheet
      @metadata ||= git.load_metadata branch: "master"
    end

    def update_spreadsheet file, message: nil
      git.update_spreadsheet(file, message: message)
    end

    def update_options filename, message=nil
      git.update_options(filename, message: message)
    end

    def options
      return @options if @options
      branch = (stage == "running") ? "master" : nil
      @options ||= git.load_options(branch: branch)
    end

    def draft?
      return (stage == 'draft')
    end

    def running?
      return (stage == 'running')
    end

    def complete?
      return (stage == 'complete')
    end

    def busy?
      prxs = proxy_states
      return true unless prxs["running"].blank? || prxs[""].blank?
    end

    def ingest?
      type == "ingest"
    end

    def update?
      type == "update"
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
      fields = []
      #    ScoobySnacks::METADATA_SCHEMA.fields.each do |field_name,field|
      ScoobySnacks::METADATA_SCHEMA['work_types']['work']['properties'].each do |field_name,field|
        fields << field_name
        fields << "#{field_name} Label" if labels && field["controlled"]
      end
      return fields
    end

    def ignored_fields
      (options['ignored headers'] || []) + IGNORED_COLUMNS
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

    def self.work_to_csv work_id, fields
      return false if work_id.empty?
      begin
        work = Work.find(work_id)
      rescue ActiveFedora::ObjectNotFoundError
        return false
      end
      line = ''
      fields.map do |field_name| 
        label = false
        if field_name.downcase.include? "label"
          label = true
          field_name = field_name[0..-7]
        end
        values = work.send(field_name)
        values.map do |value|
          next if value.is_a? DateTime 
          value = (label ? WorkIndexer.fetch_remote_label(value.id) : value.id) unless value.is_a? String
          value.gsub("\"","\"\"")
        end.join(';')
      end.join(',')
    end

    def self.filter_fields fields, label = true
      fields.each do |field_name, field|
        # reject if not in scoobysnacks
        # add label if needed
      end
    end

    def create_new_spreadsheet(fields: nil, work_ids: nil)
      work_ids ||= work_proxies.map{|proxy| proxy.work_id}
      fields ||= self.class.default_metadata_fields
      @metadata = self.class.works_to_csv(work_ids, fields)
      git.add_new_spreadsheet @metadata
    end
    
  end
end
