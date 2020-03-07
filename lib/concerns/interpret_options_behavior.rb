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
