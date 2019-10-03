class BulkOps::WorkProxy < ActiveRecord::Base

  self.table_name = "bulk_ops_work_proxies"
  belongs_to :operation, class_name: "BulkOps::Operation", foreign_key: "operation_id"
  has_many :relationships, class_name: "BulkOps::Relationship"

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

  
end
