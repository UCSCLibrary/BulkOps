module BulkOps::InterpretOptionsBehavior
  extend ActiveSupport::Concern


  def interpret_option_fields
    @raw_row.each do |field,value|
      next if value.blank? or field.blank?
      field = field.to_s
      next if value == field

      normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
      if ["visibility", "public"].include?(normfield)
        @proxy.update(visibility: format_visibility(value))

      end
      if ["worktype","model","type"].include?(normfield)
        @proxy.update(work_type: format_worktype(value) )
      end
      if ["referenceidentifier", 
          "referenceid", 
          "refid",
          "referenceidentifiertype", 
          "referenceidtype", 
          "refidtype", 
          "relationshipidentifier",
          "relationshipid",
          "relationshipidentifiertype",
          "relationshipidtype",
          "relid",
          "relidtype"].include?(normfield)
        @proxy.update(reference_identifier: format_reference_id(value))
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
  
  def format_visibility(value)
    case value.downcase
    when "public", "open", "true"
      return "open"
    when "campus", "ucsc", "institution"
      return "ucsc"
    when "restricted", "private", "closed", "false"
      return "restricted"
    end
  end

end
