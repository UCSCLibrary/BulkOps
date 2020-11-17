module BulkOps::InterpretTypeBehavior
  extend ActiveSupport::Concern

  def interpret_type_fields
    @raw_row.each do |field,value|
      normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
      if ["objecttype","model","type","worktype"].include?(normfield)
        @proxy.update(work_type: format_worktype(value) )
      end
    end

    # If the proxy is a collection, go ahead and create or link it now. Titles are unique, so we can use those as ids. 
    if @proxy.work_type.downcase == "collection"
      title = BulkOps::Parser.get_title(@raw_row)
      existing_collection = (Collection.where(title: title).select{|other_title| other_title.downcase == title.downcase} || []).first
      if existing_collection.present?      
        @proxy.work_id = existing_collection.id
      else
        new_collection = Collection.create(title: [title.to_s], depositor: operation.user.email, collection_type: Hyrax::CollectionType.find_by(title:"User Collection"))
        @proxy.work_id = new_collection.id
      end
      @proxy.save
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
