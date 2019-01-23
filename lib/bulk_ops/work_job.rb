#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

class BulkOps::WorkJob < ActiveJob::Base
  attr_accessor :status, :work, :type

  queue_as :ingest

  after_perform do |job|
    
    # update BulkOperationsWorkProxy status
    @work ||= ActiveFedora.find(@work_proxy.work_id)
    if  @work.id.nil?
      status = "error"
    else
      @work_proxy.work_id = @work.id
      status = "complete"
    end
    update_status status

    # Attempt to resolve all of the relationships defined in this row   
    @work_proxy.relationships.each do |relationship|
      relationship.resolve!
    end

    # Attempt to resolve each dangling (objectless) relationships using   
    # this work as an object
    BulkMetadata::Relationship.where(:status => "objectless").each do |relationship|
      relationship.resolve! @work.id
    end

    # Delete any UploadedFiles. These take up tons of unnecessary disk space.
    @work.file_sets.each do |fileset|
      if uf = Hyrax::UploadedFile.find_by(file: fileset.label)
        uf.destroy!
      end
    end

    # Remove any edit holds placed on an item
    @work_proxy.lift_hold

    # Check if the parent operation is finished
    # and do any cleanup if so
    @work_proxy.operation.check_if_finished
  end

  def perform(workClass,user_email,attributes,work_proxy_id,visibility="private")
    update_status "starting", "Initializing the job"
    @work_proxy = BulkOps::WorkProxy.find(work_proxy_id)
    unless @work_proxy
      report_error("Cannot find work proxy with id: #{work_proxy_id}") 
      return
    end
    if SolrDocument.find(@work_proxy.work_id)
      # The work exists in Solr. Presumably we're updating it. 
      # Report an error if we can't retrieve the work from Fedora.
      begin
        @work = ActiveFedora::Base.find(@work_proxy.work_id)
      rescue ActiveFedora::ObjectNotFoundError
        report_error "Could not find work to update in Fedora (though it shows up in Solr). Work id: #{@work_proxy.work_id}"
        return
      end
    else # The work is not found in Solr. If we're trying to update a work, we're in trouble.
      if (type == "update")
        report_error "Could not find work to update with id: #{@work_proxy.work_id}" 
        return
      end
      # Create the work we are ingesting
      @work = workClass.capitalize.constantize.new
    end
    user = User.find_by_email(user_email)
    update_status "running", "Started background task at #{DateTime.now.strftime("%d/%m/%Y %H:%M")}"
    ability = Ability.new(user)
    env = Hyrax::Actors::Environment.new(@work, ability, attributes)
    update_status "completed", Hyrax::CurationConcern.actor.send(type,env)
  end

  private

  def report_error message=nil
    update_status "job_error", message: message
  end

  def type
    #override this, setting as ingest by default
    :create
  end

  def update_status status, message=false
    return false unless @work_proxy
    @work_proxy.status = status
    @work_proxy.message = message if message
    @work_proxy.save
  end

end