module BulkOps::InterpretScalarBehavior
  extend ActiveSupport::Concern
  
  def interpret_scalar_fields
     @raw_row.each do |field, values| 
      next if values.blank? or field.nil? or field == values
      # get the field name, if this column is a metadata field
      next unless field_name = find_field_name(field.to_s)
      field = schema.get_field(field_name)
      # Ignore controlled fields
      next if field.controlled?
      BulkOps::Parser.split_values(values).each do |value|
        next if value.blank?
        value = value.strip.encode('utf-8', :invalid => :replace, :undef => :replace, :replace => '_') unless value.blank?
        value = BulkOps::Parser.unescape_csv(value)
        (@metadata[field_name] ||= []) << value
       end
    end
   end

end
