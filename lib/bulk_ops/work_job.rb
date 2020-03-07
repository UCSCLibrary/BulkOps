#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

class BulkOps::WorkJob < ActiveJob::Base
  attr_accessor :status, :work, :type

  queue_as :ingest

  after_perform do |job|
    
    # update BulkOperationsWorkProxy status
    if  @work.nil? || @work.id.nil?
      update_status "error"
    else
      @work_proxy.work_id = @work.id
      
      unless @work_proxy.work_type == "Collection"

        # If this work has a parent outside of the current operation,
        # and this is the first sibling (we only need to do this once per parent),
        # queue a job to resolve that work's new children
        if @work_proxy.parent_id.present? && (parent_proxy = BulkOps::WorkProxy.find(parent_id))
          if parent_proxy.operation_id != @work_proxy.operation_id
            if @work_proxy.previous_sibling.nil?
              BulkOps::ResolveChildrenJob.set(wait: 10.minutes).perform_later(parent_proxy.id)
            end
          end
        end

        # Set up jobs to link child works (once they are ingested)
        # or mark as complete otherwise
        if (children = @work_proxy.ordered_children)
          BulkOps::ResolveChildrenJob.perform_later(@work_proxy.id)
          update_status "awaiting_children"
        else
          update_status "complete"
        end

        # Delete any UploadedFiles. These take up tons of unnecessary disk space.
        @work.file_sets.each do |fileset|
          if uf = Hyrax::UploadedFile.find_by(file: fileset.label)
            begin
              uf.destroy!
            rescue StandardError => e
              Rails.logger.warn("Could not delete uploaded file. #{e.class} - #{e.message}")
            end
          end
        end

        # Remove any edit holds placed on an item
        @work_proxy.lift_hold
      end

      # Check if the parent operation is finished
      # and do any cleanup if so    
      if @work_proxy.operation.present? && @work_proxy.operation.respond_to?(:check_if_finished)
        @work_proxy.operation.check_if_finished 
      end
    end 
  end

  def perform(workClass,user_email,attributes,work_proxy_id,visibility=nil)
    return if status == "complete"
    update_status "starting", "Initializing the job"
    attributes['visibility']= visibility if visibility.present?
    attributes['title'] = ['Untitled'] if attributes['title'].blank?
    @work_proxy = BulkOps::WorkProxy.find(work_proxy_id)
    unless @work_proxy
      report_error("Cannot find work proxy with id: #{work_proxy_id}") 
      return
    end
    return if @work_proxy.status == "complete"

    return unless (work_action = define_work(workClass))

    user = User.find_by_email(user_email)
    update_status "running", "Started background task at #{DateTime.now.strftime("%d/%m/%Y %H:%M")}"
    ability = Ability.new(user)
    env = Hyrax::Actors::Environment.new(@work, ability, attributes)
    update_status "complete", Hyrax::CurationConcern.actor.send(work_action,env)
  end

  private


  def define_work(workClass="Work")
    if (@work_proxy.present? && @work_proxy.work_id.present? && BulkOps::SolrService.record_exists?(@work_proxy.work_id))
      begin
        @work = ActiveFedora::Base.find(@work_proxy.work_id)
        return :update
      rescue ActiveFedora::ObjectNotFoundError
        report_error "Could not find work to update in Fedora (though it shows up in Solr). Work id: #{@work_proxy.work_id}"
        return false
      end
    else
      @work = workClass.capitalize.constantize.new
      return :create
    end
  end

  def report_error message=nil
    update_status "job_error", message: message
  end

  def type
    #override this, setting as ingest by default
    :create
  end

  def update_status stat, message=false
    @status = stat
    return false unless @work_proxy
    atts = {status: stat}
    atts[:message] = message if message
    @work_proxy.update(atts)
  end

end
