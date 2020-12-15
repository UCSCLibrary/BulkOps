module BulkOps::InterpretRelationshipsBehavior
  extend ActiveSupport::Concern
  
  def interpret_relationship_fields
    @raw_row.each do |field,value|
      next if value.blank?  or field.blank? or value == field

      #the default identifier type is the reference identifier of the proxy
      id_type = reference_identifier

      # Correctly interpret the notation "parent:id", "parent id" etc in a column header
      if (split = field.split(/[:_\-\s]/)).count == 2
        id_type = split.last
        field = split.first
      end
      
      # skip to next field unless it's a known relationship field
      next unless (relationship_type = self.class.normalize_relationship_field_name(field))

      case relationship_type
      when "order"
        # If the field specifies the object's order among siblings 
        @proxy.update(order: value.to_f)
        next
      when "collection"
        # If the field specifies the name or ID of a collection,
        # find or create the collection and update the metadata to match
        add_to_collection(value)
        next
      when "parent"
        # Correctly interpret the notation "row:349", "id:s8df4j32w" etc in a cell
        if (split = value.split(/[:_\\s]/)).count == 2
          id_type = split.first
          value = split.last
        end      
        parent = find_parent_proxy(value, field, id_type)
        if parent.collection?
          add_to_collection(BulkOps::Parser.get_title(@raw_data[parent.row_number]))
        else
          proxy_updates =  { parent_id: parent.id}
          siblings = parent.ordered_children
          if siblings.present? && @proxy.previous_sibling_id.nil?
            proxy_updates[:previous_sibling_id] = siblings.last.id
          end
          @proxy.update(proxy_updates)  
        end
      end
    end
  end

  private

  def add_to_collection collection_name
    collection = find_or_create_collection(collection_name)
    ( @metadata[:member_of_collection_ids] ||= [] ) << collection.id if collection.present?
  end

  def find_previous_parent_row field="parent"
    #Return the row number of the most recent preceding row that does
    # not itself have a parent defined
    i = 1;
    while (prev_row = raw_data[row_number - i])
      return (row_number - i) if prev_row[field].blank?
      i += 1
    end
  end

  def find_parent_proxy parent_id, field, id_type
    #The id_type determines what kind of identifier we expect in parent_id
    case id_type.downcase
    when "id"
      # Expect a reference to an existing work in the DAMS
      return false unless BulkOps::SolrService.record_exists?(parent_id.to_s)
      # Pull the work proxy for that work, if it exists
      parent_proxy = BulkOps::WorkProxy.find_by(work_id: parent_id.to_s, operation_id: @proxy.operation.id) || BulkOps::WorkProxy.find_by(work_id: parent_id.to_s)
      # If no work proxy exists for this work, create one just to keep track of this task
      return parent_proxy if proxy.present?
      return BulkOps::WorkProxies.create(status: "awaiting_children",
                                         operation_id: 0,
                                         last_event: DateTime.now,
                                         work_id: parent_id.to_s)
        
    when "proxy_id"
      return BulkOps::WorkProxy.find(parent_id)
    when "row"
      if parent_id =~ /\A[-+]?[0-9]+\z/
        if parent_id.to_i < 0
          # if given a negative integer, count backwards from the current row (remember that parent_id.to_i is negative)
          parent_id = @proxy.row_number.to_i + parent_id.to_i
        elsif parent_id.to_i > 0
          # if given a positive integer, just remove the row offset
          parent_id = parent_id.to_i - BulkOps::ROW_OFFSET
        end
      elsif parent_id.to_s.downcase.include?("prev")
        # if given any variation of the word "previous", get the first preceding row with no parent of its own
        parent_id = find_previous_parent_row(field)
      end
      
      return BulkOps::WorkProxy.find_by(operation_id: @proxy.operation_id, 
                                        row_number: parent_id.to_i)
      #    when "title"
      #      #          TODO clean up solr query and add work type to it
      #      query = "{!field f=title_tesim}#{object_identifier}"
      #      objects = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path,
      #                                                            params: { fq: query, rows: 1})["response"]["docs"]
      #      return ActiveFedora::Base.find(objects.first["id"]) if objects.present?
      #      return false
      #    when "identifier"
      #      query = "{!field f=identifier_tesim}#{object_identifier}"
      #      objects = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path,params: { fq: query, rows: 100})["response"]["docs"]
      #      return false if objects.blank?
      #      return ActiveFedora::Base.find(objects.first["id"])
    end        
  end

  def find_collection(collection)
    cols = Collection.where(title: collection)
    cols += Collection.where(title: collection).select{|col| col.title.first == collection}
    cols += Collection.where(id: collection)
    return cols.last unless cols.empty?
    return false
  end

  def find_or_create_collection(collection)
    find_collection(collection) || Collection.create(title: [collection.to_s], depositor: operation.user.email, collection_type: Hyrax::CollectionType.find_by(title:"User Collection"))
  end


end
