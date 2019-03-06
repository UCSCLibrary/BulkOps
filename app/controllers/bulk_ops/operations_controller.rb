module BulkOps
  class OperationsController < ApplicationController

    skip_before_action :verify_authenticity_token, only: [:apply]

    before_action :define_presenter
    before_action :github_auth
    load_and_authorize_resource
    before_action :initialize_options, only: [:new,:show,:edit, :update]
    before_action :initialize_operation, only: [:edit, :destroy, :show, :request_apply, :approve, :csv, :errors, :log, :update, :request, :duplicate]

    layout 'hyrax/dashboard'
    attr_accessor :git

    helper :hyrax
    helper_method :create_work_presenter

    # Hacky fix for bug displaying masthead. 
    # This will change anyway with next Hyrax upgrade.
    def create_work_presenter
      return []
    end

    def index
      branches = BulkOps::GithubAccess.list_branch_names current_user
      BulkOps::Operation.all.each{|op| op.destroy! unless branches.include?(op.name) }
      @active_operations = BulkOps::Operation.where.not(stage: ['completed','discarded'])
      @active_operations.each {|op| op.destroy! unless branches.include?(op.name) }
      @old_operations = BulkOps::Operation.where(stage: ['completed','discarded']) if params["show_old_ops"]
    end

    def delete_all
      unless operation.type == "ingest"
        redirect_to action: "show", id: @operation.id, error: "Can only delete all works from an ingest operation, not an #{operation.type}" 
      end
      # delete all the works
      operation.delete_all
      flash[:notice] = "All works created by this ingest have been deleted from the system. Feel free to re-apply the ingest."
      redirect_to action: "show", id: @operation.id
    end

    def destroy_multiple
      params['operation_ids'].each{|id| BulkOps::Operation.find(id).destroy! }
      flash[:notice] = "Bulk operations deleted successfully"
      redirect_to action: "index"
    end

    def duplicate 
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end
      case params['status_to_edit']
      when "all"
        proxies = @operation.work_proxies
      when "error", "errors","failed"
        proxies = @operation.work_proxies.where(status: "error")
        proxies += @operation.work_proxies.where(status: "errors")
      when "complete"
        proxies = @operation.work_proxies.where(status: "complete")
      else
        proxies = @operation.work_proxies
      end

      message = params['git_message'] || "This update was created from a group of works affected by a previous operation."

#      work_ids = proxies.map{|prx| prx.id}
      name_base = (params['name'] || @operation.name).parameterize
      name = BulkOps::Operation.unique_name(name_base, current_user)
      new_operation = BulkOps::Operation.create(name: name, 
                                                status: "new", 
                                                stage: "new", 
                                                operation_type: 'update', 
                                                message: message, 
                                                user: current_user)
      
      new_operation.create_branch fields: params['fields'],  options: filtered_options_params
      new_operation.status = "OK"
      new_operation.stage = "draft"
      new_operation.work_proxies = proxies
      new_operation.message = "New update draft created. Ready for admins to select which works to edit."

      new_operation.save
      
      flash[:notice] = "Successfully created a new bulk update from the works involved in the previous operation."
      redirect_to action: "show", id: new_operation.id
    end

    def create
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "index"
      end

      params.require([:name,:type,:notified_users])

      # Create a unique operation name if the chosen name is taken
      op_name = BulkOps::Operation.unique_name(params['name'].parameterize, current_user)
      
      message = params['git_message'] || "This #{params['type']} is brand new, just created by #{current_user.email}"

      operation = BulkOps::Operation.create(name: op_name, 
                                            status: "new", 
                                            stage: "new", 
                                            operation_type: params['type'], 
                                            message: message, 
                                            user: current_user)

      operation.status = "OK"

      puts "BULK OPS CREATING BRANCH WITH OPTIONS: #{filtered_options_params.inspect}"
      puts "ALL PARAMS: #{params.inspect}"

      operation.create_branch fields: params['fields'], options: filtered_options_params, operation_type: params['type'].to_sym

      case params['type']
      when "ingest"
        operation.stage = "pending"
        operation.message = "Generated blank ingest spreadsheet and created Github branch for this ingest"
      when "update"
        operation.stage = "draft"
        operation.message = "New update draft created. Ready for admins to select which works to edit."
      end

      operation.save

      #redirect to operation show page
      flash[:notice] = "Bulk #{params['type']} created successfully"
      redirect_to action: "show", id: operation.id
    end

    def new
      @default_fields = BulkOps::Operation.default_metadata_fields + ['id','collection','filename']
      @all_fields = (BulkOps::Operation.default_metadata_fields + BulkOps::Operation::SPECIAL_COLUMNS)
    end

    def show
      if @operation.running? || @operation.complete?
        @num_works = (cnt = @operation.work_proxies.count) > 0 ? cnt : 1
        @num_queued = @operation.work_proxies.where(status: 'queued').count
        @num_running = @operation.work_proxies.where(status: 'running').count
        @num_failed = @operation.work_proxies.where(status: 'failed').count
        @num_complete = @operation.work_proxies.where(status: 'complete').count
        @num_other = @operation.work_proxies.where.not(status: ['queued','running','failed','complete']).count
      end
      if @operation.stage=="draft"
        @draft_work_count = @operation.work_proxies.count
        @draft_works = @operation.work_proxies.take(10).map{|proxy| get_solr_doc(proxy.work_id)}.select{|doc| doc}
      end
    end

    def update
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end
      
#      params.permit(available_options).permit(ignored_columns: []).permit("edit_options")

      #if a new spreadsheet is uploaded, put it in github
      if params['spreadsheet'] && @operation.name
        @operation.update_spreadsheet params['spreadsheet'], message: params['git_message']
        flash[:notice] = "Spreadsheet updated successfully"
        redirect_to action: "show", id: @operation.id
      end

      #If new options have been defined, update them in github
      if params['edit_options']
        if filtered_options_params && @operation.name

          options = @operation.options

          puts "BULKOPS UPDATING OPTIONS"
          puts "all params: #{params.inspect}"
          puts "Updating bulk_ops options"
          puts "new options:"
          puts filtered_options_params.inspect
          puts "old options:"
          puts options.inspect


          filtered_options_params.each do |option_name, option_value|
            options[option_name] = option_value
          end

          puts "final options:"
          puts options.inspect

          BulkOps::GithubAccess.update_options(@operation.name, options, message: params['git_message'])
          flash[:notice] = "The operation options were updated successfully"
          redirect_to action: "show", id: @operation.id
        else
          redirect_to action: "show", id: @operation.id
        end
      end
      destroy if params["destroy"]
      finalize_draft if params["finalize"]
    end

    def finalize_draft
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end
      @operation.finalize_draft
      redirect_to action: "show"
    end
    
    def edit
      redirect_to action: "show", id: @operation.id unless @operation.draft?
      added = false
      destroyed = false

      if params['add_all_results'].to_s == "true"
        rows = 100
        builder = BulkOps::SearchBuilder.new(scope: self,
                                             collection: params['collection'],
                                             collection_id: params['collection_id'],
                                             admin_set: params['admin_set'],
                                             admin_set_id: params['admin_set_id'],
                                             workflow_state: params['workflow_state'],
                                             keyword_query: params['q']).rows(rows)
        result = repository.search(builder)
        total = result.total
        start = 0
        while start < total
          builder.start = start
          docs = repository.search(builder).documents
          docs.each do |doc|
            unless BulkOps::WorkProxy.find_by(operation_id: @operation.id, work_id: doc.id)
              BulkOps::WorkProxy.create(operation_id: @operation.id, 
                                        work_id: doc.id,
                                        status: "new",
                                        last_event: DateTime.now,
                                        message: params['git_message'] || "Works added to update by #{current_user.name || current_user.email}")
              added = true
            end
          end
          start += rows
        end
        
      elsif params['added_work_ids'] 
        added = false

        params['added_work_ids'].each do |work_id|
          next if work_id.blank?
          unless BulkOps::WorkProxy.find_by(operation_id: @operation.id, work_id: work_id)
            BulkOps::WorkProxy.create(operation_id: @operation.id, 
                                      work_id: work_id,
                                      status: "new",
                                      last_event: DateTime.now,
                                      message: params['git_message'] || "Works added to update by #{current_user.name || current_user.email}")
            added = true
          end
        end
      end

      if params['remove_works'] && params['remove_work_ids']
        destroyed = false
        params['remove_work_ids'].each do |work_id|
          if (proxy = BulkOps::WorkProxy.find_by(operation_id: @operation.id, work_id: work_id))
            proxy.destroy!
            destroyed = true
          end
        end
      end

      flash[:notice] = "Works removed successfully from update" if destroyed
      flash[:notice] = "Works added successfully to update" if added
      redirect_to action: "show", id: @operation.id
    end

    def find_record_set type, identifier
      return unless identifier.is_a? Integer
      type = type.capitalize.constantize
      #    begin
      record = type.find(identifier)
      #    rescue ActiveFedora::ObjectNotFoundError
      #      record = nil
      #    end
      return record
    end

    def search
      start = (params['start'] || 0).to_i
      rows = (params['rows'] || 15).to_i
      builder = BulkOps::SearchBuilder.new(scope: self,
                                           collection: params['collection'],
                                           collection_id: params['collection_id'],
                                           admin_set: params['admin_set'],
                                           admin_set_id: params['admin_set_id'],
                                           workflow_state: params['workflow_state'],
                                           keyword_query: params['q'])
      builder = builder.rows(rows).start(start)
      result = repository.search(builder)
      response.headers['Content-Type'] = 'application/json'
      render json: {num_results: result.total, results: result.documents}
    end

    def destroy
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end

      @operation.destroy!
      flash[:notice] = "Bulk #{@operation.type} deleted successfully"
      redirect_to action: "index"
    end

    def request_apply
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end

      @operation.stage = "verifying"
      @operation.save
      BulkOps::VerificationJob.perform_later(@operation)
      flash[:notice] = "We are now running the data from your spreadsheet through an automatic verification process to anticipate any problems before we begin the ingest. This may take a few minutes. You should recieve an email when the process completes."
      redirect_to action: "show"
    end
    
    def approve
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end

      begin
        @operation.merge_pull_request @operation.pull_id, message: params['git_message']
        @operation.delete_branch
        @operation.stage = "waiting"
        @operation.save
      rescue Octokit::MethodNotAllowed 
        flash[:error] = "For some reason, github says that it won't let us merge our change into the master branch. This is strange, since it passed our internal verification. Log in to github and check out the branch manually to see if there are any strange files or unexplained commits.}"
      rescue 
        flash[:error] = "There was a confusing error while merging our github branch into the master branch. Please log in to Github and check whether the pull request was approved."
      end
      redirect_to action: "show"
    end
    
    def apply
      unless @github_authenticated
        flash[:notice] = "Please log in to github before taking this action" 
        redirect_to action: "show", id: @operation.id
      end

      parameters = JSON.parse request.raw_post
      unless parameters['action'] == 'closed' 
        render plain: "IGNORING THIS EVENT" 
        return
      end
      if parameters['pull_request'].blank?
        render plain: "Error - the pull request was not passed from Github", status: 400 
        return
      end
      if parameters['pull_request']['merged'].to_s.downcase == "false"
        render plain: "IGNORING THIS EVENT" 
        return
      end
      verify_github_signature
      op = BulkOps::Operation.find_by(pull_id: parameters['number'])
      puts "if operation \"#{op.name}\" isn't running, it'll get applied now"
      unless ["running","complete"].include? op.stage
        op.apply!
        flash[:notice] = "Applying bulk #{op.operation_type}. Stay tuned to see how it goes!"
      end
      render plain: "OK"
    end

    def csv
      response.headers['Content-Type'] = 'text/csv'
      response.headers['Content-Disposition'] = "attachment; filename=#{@operation.name}.csv"    
      render :text => @operation.get_spreadsheet(return_headers: true)
    end

    def blacklight_config
      CatalogController.blacklight_config
    end

    private

    def verify_github_signature
      signature = 'sha1=' + OpenSSL::HMAC.hexdigest(OpenSSL::Digest.new('sha1'), BulkOps::GithubAccess.webhook_token, request.raw_post)
      return halt 500, "Signatures didn't match!" unless Rack::Utils.secure_compare(signature, request.headers['X-HUB-SIGNATURE'])
    end

    def available_options
      available_options = [:name,
                           :fields,
                           :file_prefix,
                           :ignored_columns, 
                           :complex_objects, 
                           :notified_users,
                           :type,
                           :name,
                           :work_type,
                           :file_method,
                           :visibility,
                           :reference_identifier,
                           :reference_column_name,
                           :creator_email]
    end

    def filtered_options_params
      #TODO Verify work_types, visibilities,filename_prefixes,etc
      params.permit(*available_options, ignored_columns: [])
    end

    def initialize_options
      @file_update_options = [["Update metadata only. Do not change files in any way.",'metadata-only'],
                              ["Remove all files attached to these works and ingest a new set of files",'replace-all'],
                              ["Re-ingest some files with replacements of the same filename. Leave other files alone.","reingest-some"],
                              ["Remove one list of files and ingest another list of files. Leave other files alone.","remove-and-add"]]
      @visibility_options = [["Public",'open'],
                             ["Private",'restricted'],
                             ["UCSC","ucsc"]]
      #TODO pull from registered work types
      @work_type_options = [["Work",'Work'],
                            ["Course","Course"],
                            ["Lecture",'Lecture']]

      default_notifications = [current_user,User.first].uniq
      
      @notified_users = params['notified_user'].blank? ? default_notifications : params['notified_user'].map{ |user_id| User.find(user_id.to_i)}
    end

    def get_solr_doc id
      solr_doc = false
      begin
        return solr_doc = SolrDocument.find(id)
      rescue Blacklight::Exceptions::RecordNotFound
        return false
      end
      return solr_doc
    end

    def initialize_operation

      #define branch options if a branch is not specified
      if @operation.nil?
        @branch_names = BulkOps::GithubAccess.list_branch_names current_user
        @branch_options = @branch_names.map{|branch| [branch,branch]}
        @branch_options = [["No Bulk Updates Defined",0]] if @branch_options.blank?
      elsif @operation.stage == "draft" 
        @works = @operation.work_proxies.map{|proxy| get_solr_doc(proxy.work_id)}.select{|doc| doc}
        @collections = Collection.all.map{|col| [col.title.first,col.id]}
        @admin_sets = AdminSet.all.map{|aset| [aset.title.first, aset.id]}
        workflow = Sipity::Workflow.where(name:"ucsc_generic_ingest").last
        unless workflow.active?
          workflow.active = true
          workflow.save
        end
        @workflow_states = workflow.workflow_states.map{|st| [st.name, st.id]}
      end
    end
    
    #TODO
    def esure_admin!
      # Do appropriate user authorization for this. Based on workflow roles / privileges? Or just user roles?
    end

    def define_presenter
      @presenter = Hyrax::Admin::DashboardPresenter.new
    end

    def github_auth
      @github_username = BulkOps::GithubAccess.username current_user
      @github_authenticated = @github_username.is_a? String
      cred = BulkOps::GithubCredential.find_by(user_id: current_user.id)
      @auth_url = BulkOps::GithubAccess.auth_url current_user
      session[:git_auth_redirect] = request.original_url
    end

    def repository
      @repository ||= Blacklight::Solr::Repository.new(CatalogController.blacklight_config)
    end

  end
end
