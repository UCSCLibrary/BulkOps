class BulkOps::ResolveChildrenJob < ActiveJob::Base

  def perform(proxy_id)
    proxy = BulkOps::WorkProxy.find(proxy_id)
    if proxy.work_id.present? && proxy.ordered_children.all?{|child| child.work_id.present?}
      work = ActiveFedora::Base.find(proxy.work_id)
      work.save
      work.ordered_members = proxy.ordered_children.map{|proxy| ActiveFedora::Base.find(proxy.work_id)}
      work.save
      proxy.ordered_children.each{|proxy| ActiveFedora::Base.find(proxy.work_id).save}
    else
      BulkOps::ResolveChildrenJob.set(wait: 30.minutes).perform_later(proxy_id)
    end
  end

end
