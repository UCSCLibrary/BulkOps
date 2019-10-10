class BulkOps::Relationship < ActiveRecord::Base
  RELATIONSHIP_FIELDS = ['parent','child','order','next','collection']

  self.table_name = "bulk_ops_relationships"
  belongs_to :work_proxy, class_name: "BulkOps::WorkProxy", foreign_key: "work_proxy_id"
  delegate :operation, :operation_id, to: :work_proxy

  def initialize *args
    super *args
    
    # Attempt to resolve the relationship immediately
    # which might work in the case of updates
#    resolve!
  end

  def findObject
    case (identifier_type || "").downcase
    when "id"
      begin
      object = ActiveFedora::Base.find(object_identifier)
      rescue Ldp::Gone
        return false
      end
      return object || false
    when "title"
      #          TODO clean up solr query and add work type to it
      query = "{!field f=title_tesim}#{object_identifier}"
      objects = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path,
                                                            params: { fq: query, rows: 100})["response"]["docs"]
      if objects.present?
        return ActiveFedora::Base.find(objects.first["id"])
      elsif (relationship_type || "").downcase == "collection"
        return Collection.create(title: [object_identifier])
      else
        return false
      end
    when "identifier"
      query = "{!field f=identifier_tesim}#{object_identifier}"
      objects = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path,params: { fq: query, rows: 100})["response"]["docs"]
      return false if objects.blank?
      return ActiveFedora::Base.find(objects.first["id"])
    when "row"
      object_proxy = BulkOps::WorkProxy.find_by(operation_id: work_proxy.operation_id, 
                                                row_number: (object_identifier.to_i))
      ActiveFedora::Base.find(object_proxy.work_id)
    when "proxy_id"
      return false unless (proxy = BulkOps::WorkProxy.find(proxy_id))
      return false unless proxy.work_id.present?
      ActiveFedora::Base.find(proxy.work_id)
    end
  end

  def resolve!
    unless subject = work_proxy.work and object = self.findObject
      wait!
      return
    end
    implement_relationship! relationship_type, subject, object
  end

  def insert_among_children(object,new_member)
    return nil unless ["parent"].include?((relationship_type || "").downcase)
    prev_sib_id = previous_sibling
    # This is the id of the WorkProxy associate with the most recent sibling work
    # that might be fully ingested. If is it not fully ingested, we will move on 
    # to the preceding sibling.
    while prev_sib_id.present? 
      prev_sib_proxy = BulkOps::WorkProxy.find(prev_sib_id)
      # Check if the previous sibling is fully ingested 
      # and get its index among its siblings (if it has been successfully attached to the parent)
      prev_sib_index = object.ordered_member_ids.index(prev_sib_proxy.work_id) if prev_sib_proxy.work_id.present?
      # Insert the new member among its siblings if we found the right place
      return object.ordered_members.to_a.insert(prev_sib_index+1, new_member) if prev_sib_index.present?
      # Otherwise, pull up the sibling's relationship field to check if it sibling has a sibling before it
      sib_relationship = prev_sib_proxy.relationships.find{|rel| rel.findObject.id == object.id }
      # If we can't find an ingested sibling among the ordered members,
      # break this loop and make this work the first member.
      break unless sib_relationship.present?
      prev_sib_id = sib_relationship.previous_sibling
    end
    #If we never found an existing previous sibling already attached, put this one at the front
    return  [new_member]+object.ordered_members.to_a
  end
  
  def implement_relationship!(type,subject,object)
    case (type || "").downcase
    when "parent"
      unless object.member_ids.include? subject.id
        object.reload
        object.save
        object.ordered_members = insert_among_children(object, subject)
        object.save
      end
    when "child"
      #CAVEAT ordering not fully implemented in this case
      unless subject.member_ids.include? object.id
        subject.ordered_members << object
        subject.save
      end
    when "order"
      #TODO - implement this - related to ordering of filesets
      
    end
    update(status: "complete")
  end
  
  private 

  def fail!
    update(status: "failed")
  end
  
  def wait!
    update(status: "pending")
  end
  
end
