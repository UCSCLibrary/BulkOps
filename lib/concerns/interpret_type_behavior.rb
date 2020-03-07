module BulkOps::InterpretTypeBehavior
  extend ActiveSupport::Concern

  def interpret_type_fields
    @raw_row.each do |field,value|
      if ["worktype","model","type"].include?(normfield)
        @proxy.update(work_type: format_worktype(value) )
      end
    end
  end

  private 

  def format_worktype(value)
    # format the value like a class name
    type = value.titleize.gsub(/[-_\s]/,'')
    # reject it if it isn't a defined class
    type = false unless Object.const_defined? type
    # fall back to the work type defined by the operation, or a standard "Work"
    return type ||= work_type || operation.work_type || "Work"
  end
  
end
