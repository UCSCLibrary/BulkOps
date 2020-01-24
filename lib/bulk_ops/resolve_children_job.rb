class BulkOps::ResolveChildrenJob < ActiveJob::Base

  def perform(proxy_id)
    proxy = BulkOps::WorkProxy.find(proxy_id)
    if proxy.ordered_children.all?{|child| child.work_id.present?}
      work = ActiveFedora::Base.find(proxy.work_id)
      work.ordered_member_ids = proxy.ordered_children.map(&:work_id)
      work.save
    else
      BulkOps::ResolveChildrenJob.set(wait: 30.minutes).perform_later(proxy_id)
    end
  end

end
