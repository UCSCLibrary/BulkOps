#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

require 'bulk_ops/work_job'

class BulkOps::UpdateWorkJob < BulkOps::WorkJob

  private

  def type
    :update
  end

  def define_work workClass=nil
    # report an error if we can't find the work in solr
    unless BulkOps::SolrService.record_exists?(@work_proxy.work_id)
      report_error "Could not find work to update with id: #{@work_proxy.work_id} referenced by work proxy: #{@work_proxy.id}"  
      return false
    end
    # Report an error if we can't retrieve the work from Fedora.
    begin
      @work = ActiveFedora::Base.find(@work_proxy.work_id)
    rescue ActiveFedora::ObjectNotFoundError
      report_error "Could not find work to update in Fedora (though it shows up in Solr). Work id: #{@work_proxy.work_id}"
      return false
    end
    return @work
  end

end
