class BulkOps::WorkProxy < ActiveRecord::Base

  self.table_name = "bulk_ops_work_proxies"
  belongs_to :operation, class_name: "BulkOps::Operation", foreign_key: "operation_id"

  attr_accessor :proxy_errors

  def initialize *args
    super *args
    place_hold if @work_id
  end

  def work
    return @work if @work
    begin
      @work = ActiveFedora::Base.find(work_id)
    rescue 
      return false
    end
    return @work
  end

  def work_type
    super || operation.work_type || "Work"
  end

  def place_hold
    # TODO make it so nobody can edit the work
  end

  def lift_hold
    # TODO make it so people can edit the work again
  end


  def proxy_errors
    @proxy_errors ||= []
  end

  def ordered_siblings
    return nil unless (parent = BulkOps::WorkProxy.find(parent_id))
    parent.ordered_children - self
  end

  def ordered_children
    children = BulkOps::WorkProxy.where(parent_id: id)
    ordered_kids = []
    previous_id = nil
    while ordered_kids.length < children.length do
      next_child = children.find{|child| child.previous_sibling_id == previous_id}
      break if (next_child.nil? or ordered_kids.include?(next_child))
      previous_id = next_child.id
      ordered_kids << next_child
    end
    ordered_kids = ordered_kids + (children - ordered_kids) if (children.length > ordered_kids.length)
    ordered_kids
  end
end
