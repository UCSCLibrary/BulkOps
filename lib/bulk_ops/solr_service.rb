class BulkOps::SolrService
  
  def self.record_exists? id
    begin
      return true if SolrDocument.find(id)
    rescue Blacklight::Exceptions::RecordNotFound
      return false
    end
    return false
  end


end
