class BulkOps::ResolveChildrenJob < ActiveJob::Base

  def perform(proxy_id)
    proxy = BulkOps::WorkProxy.find(proxy_id)
    if proxy.work_id.present? && proxy.ordered_children.all?{|child| child.work_id.present?}
      work = ActiveFedora::Base.find(proxy.work_id)
      work.save
      ordered_file_sets = (work.ordered_member_ids & work.file_set_ids).map{|id| ActiveFedora::Base.find(id)}
      work.ordered_members = proxy.ordered_children.map{|proxy| ActiveFedora::Base.find(proxy.work_id)} + ordered_file_sets
      work.save
      proxy.ordered_children.each{|proxy| ActiveFedora::Base.find(proxy.work_id).save}
    else
      BulkOps::ResolveChildrenJob.set(wait: 30.minutes).perform_later(proxy_id)
    end
  end

end
