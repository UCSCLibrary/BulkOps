class BulkOps::QueueWorkIngestsJob <  ActiveJob::Base
  attr_accessor :operation

  queue_as :ingest

  def perform(op)
    @operation = op
    metadata = op.final_spreadsheet
    op.work_proxies.where(status:'queued').each do |proxy|
      data = proxy.interpret_data(metadata[proxy.row_number])
      next unless proxy.proxy_errors.blank?
      proxy.update(status: 'sidekiq', message: "interpreted at #{DateTime.now.strftime("%d/%m/%Y %H:%M")} " + proxy.message)
      BulkOps::CreateWorkJob.perform_later(proxy.work_type || "Work",
                                           op.user.email,
                                           data,
                                           proxy.id,
                                           proxy.visibility)
    end
  end

end
