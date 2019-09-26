class BulkOps::Relationship < ActiveRecord::Base
  RELATIONSHIP_FIELDS = ['parent','child','order','next','collection']

  self.table_name = "bulk_ops_relationships"
  belongs_to :work_proxy, class_name: "BulkOps::WorkProxy", foreign_key: "work_proxy_id"
  delegate :operation, :operation_id, to: :work_proxy

  def initialize *args
    super *args
    
    # Attempt to resolve the relationship immediately
    # which might work in the case of updates
    resolve!
  end

  def findObject
    case identifier_type.downcase
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
      elsif relationship_type.downcase == "collection"
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
                                                row_number: (object_identifier.to_i - 2))
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

  def insert_among_siblings(ordered_members,new_member)
    return nil unless ["parent"].include?(relationship_type.downcase)
    prev_sib_id = previous_sibling
    while prev_sib_id.present? 
      prev_sib_proxy = BulkOps::WorkProxy.find(previous_sibling)
      # check if the previous sibling has been fully ingested
      if prev_sib_proxy.work_id.present?
        # Check if the previous sibling is attached to the parent
        prev_member = ordered_members.find{|member| member.id == prev_sib_proxy.work_id}
        # Insert the new member among its siblings at the right place
        return ordered_members.insert(ordered_members.index(prev_member)+1, new_member) if prev_member.present?
      end
      # Otherwise, check if this sibling has a sibling before it
      sib_relationship = prev_sib_proxy.relationships.find_by(relationship_type: relationship_type, object_identifier: object_identifier)
      # If we can't find an ingested sibling among the ordered members,
      # make this work the first member.
      break unless sib_relationship.present?
      prev_sib_id = sib_relationship.previous_sibling
    end
    return  [new_member]+ordered_members
  end
  
  def implement_relationship!(type,subject,object)
    case type.downcase
    when "parent"
      unless object.member_ids.include? subject.id
        object.ordered_members = insert_among_siblings(ordered_members, new_member)
        object.save
      end
    when "child"
      #CAVEAT ordering not fully implemented in this case
      unless subject.member_ids.include? object.id
        subject.ordered_members << object
        subject.save
      end
    when "collection"
      unless object.member_object_ids.include? subject.id
        object.add_members([subject.id])
        object.save
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
