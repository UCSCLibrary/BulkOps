class BulkOps::Relationship < ActiveRecord::Base
  RELATIONSHIP_FIELDS = ['parent','child','order','next','collection']

  self.table_name = "bulk_ops_relationships"
  belongs_to :work_proxy, class_name: "BulkOps::WorkProxy", foreign_key: "work_proxy_id"

  def initialize *args
    super *args
    
    # Attempt to resolve the relationship immediately
    # which might work in the case of updates
    resolve!
  end

  def findObject
    case identifier_type
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
                                                            params: { fq: query, rows: 100})["response"]["docs"].first
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
      object_proxy = BulkOps::WorkProxy.find_by(operation_id: work_proxy.operation.id, 
                                                row_number: (object_identifier.to_i - 2))
      ActiveFedora::Base.find(object_proxy.work_id)
    end
  end

  def resolve!
    unless subject = work_proxy.work and object = self.findObject
      wait!
      return
    end
    implement_relationship! relationship_type, subject, object
  end
  
  def implement_relationship!(type,subject,object)
    case type
    when "parent"
      object.ordered_members << subject
      object.save
    when "child"
      subject.ordered_members << object
      subject.save
    when "collection"
      object.add_members([subject.id])
      object.save
    when "next"
    #TODO - implement this - related to ordering of filesets
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
