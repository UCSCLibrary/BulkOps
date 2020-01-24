class BulkOps::ApplyOperationJob <  ActiveJob::Base
  queue_as :ingest

  def perform(op_id)
    BulkOps::Operation.find(op_id).apply
  end

end
