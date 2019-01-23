#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

require 'bulk_ops/work_job'

class BulkOps::UpdateWorkJob < BulkOps::WorkJob

  private

  def type
    :update
  end

end
