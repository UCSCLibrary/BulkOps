#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

require 'bulk_ops/work_job'

class BulkOps::CreateWorkJob < BulkOps::WorkJob

  private

  def type
    :create
  end

  def define_work workClass
    if record_exists?(@work_proxy.work_id)
        report_error "trying to ingest a work proxy that already has a work attached. Work id: #{@work_proxy.work_id} Proxy id: #{@work_proxy.id}" 
        return false
    end
    @work = workClass.capitalize.constantize.new
  end

end
