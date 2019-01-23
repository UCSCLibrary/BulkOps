#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

require 'bulk_ops/work_job'

class BulkOps::CreateWorkJob < BulkOps::WorkJob

  private

  def type
    :create
  end

end
