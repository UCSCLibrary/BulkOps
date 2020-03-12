class BulkOps::Parser
  require 'uri'

  attr_accessor :proxy, :raw_data, :raw_row

  delegate :relationships, :operation, :row_number, :work_id, :visibility, :work_type, :reference_identifier, :order, to: :proxy

  include BulkOps::InterpretRelationshipsBehavior
  include BulkOps::InterpretFilesBehavior
  include BulkOps::InterpretScalarBehavior
  include BulkOps::InterpretOptionsBehavior
  include BulkOps::InterpretControlledBehavior
  include BulkOps::InterpretTypeBehavior

  def self.unescape_csv(value)
    value.gsub(/\\(['";,])/,'\1')
  end

  def self.split_values value_string
    # Split values on all un-escaped separator character (escape character is '\')
    # Then replace all escaped separator charactors with un-escaped versions
    value_string.split(/(?<!\\)#{BulkOps::SEPARATOR}/).map{|val| val.gsub("\\#{BulkOps::SEPARATOR}",BulkOps::SEPARATOR).strip}
  end

  def self.normalize_relationship_field_name field
    normfield = field.to_s.downcase.parameterize.gsub(/[_\s-]/,'')
    BulkOps::RELATIONSHIP_FIELDS.find{|rel_field| normfield == rel_field }
  end

  def self.is_file_set? metadata, row_number
    return false unless metadata[row_number].present?
    # If the work type is explicitly specified, use that
    if (type_key = metadata[row_number].to_h.keys.find{|key| key.to_s.downcase.gsub(/[_\-\s]/,"").include?("worktype") })
      return true if metadata[row_number][type_key].downcase == "fileset" 
      return false if metadata[row_number][type_key].present?
    end
#    Otherwise, if there are any valid fields other than relationship or file fields, call it a work
    metadata[row_number].each do |field, value|
      next if BulkOps::Verification.is_file_field?(field)
      next if ["parent", "order"].include?(normalize_relationship_field_name(field))
      next if ["title","label"].include?(field.downcase.strip)
      return false
    end
    return true
  end

 def self.get_negating_metadata(work_id, metadata={})
    return false unless BulkOps::SolrService.record_exists?(work_id)
    work = ActiveFedora::Base.find(work_id)
    schema = ScoobySnacks::METADATA_SCHEMA
    schema.all_fields.each do |field|
      field_key = field.controlled? ? "#{field.name}_attributes" : field.name
      metadata[field_key] ||= (field.multiple? ? [] : nil)
      if field.controlled?        
        values = Array(work.send(field.name)).map{|value| {id: value.id, _destroy: true} }
        if field.multiple?
          metadata[field_key] += values
        else
          metadata[field_key] = values.first
        end
      end
    end
    return metadata
  end

  def initialize prx, metadata_sheet=nil, options={}
    @proxy = prx
    @raw_data = (metadata_sheet || operation.metadata)
    @raw_row = @raw_data[@proxy.row_number]
    @metadata = {}
    @parsing_errors = []
    @options = options || operation.options
  end

  def interpret_data raw_row: nil, raw_data: nil, proxy: nil
    @raw_row = raw_row if raw_row.present?
    @proxy = proxy if proxy.present?
    @raw_data = raw_data if raw_data.present?
    disambiguate_columns
    setAdminSet
    #The order here matters a little: interpreting the relationship fields specifies containing collections,
    # which may have opinions about whether we should inherit metadata from parent works
    interpret_relationship_fields
    setMetadataInheritance
    interpret_option_fields
    interpret_type_fields
    if @proxy.work_id.present? && @options['discard_existing_metadata']
      @metadata.deep_merge!(self.class.get_negating_metadata(@proxy.work_id))
    end
    interpret_file_fields unless @proxy.collection?
    interpret_controlled_fields
    interpret_scalar_fields
    connect_existing_work
    @proxy.update(status: "ERROR", message: "error parsing spreadsheet line") if @parsing_errors.present?
    @proxy.proxy_errors = (@proxy.proxy_errors || []) + @parsing_errors
    return @metadata
  end

  def disambiguate_columns
    #do nothing unless there are columns with the same header
    return unless (@raw_row.respond_to?(:headers) && (@raw_row.headers.uniq.length < @raw_row.length) )
    row = {}
    (0...@raw_row.length).each do |i|
      header = @raw_row.headers[i]
      value = @raw_row[i]
      next unless value.present?
      # separate values in identical columns using the separator
      row[header] = (Array(row[header]) << value).join(BulkOps::SEPARATOR)
    end
    @raw_row = row
  end

  def connect_existing_work
    if @proxy.collection?
      cols = Collection.where(title: @metadata['title'].first)
      work_id = cols.first.id unless cols.blank?
    else
      return unless (column_name = @options["update_identifier"])
      return unless (key = @raw_row.to_h.keys.find{|key| key.to_s.parameterize.downcase.gsub("_","") == column_name.to_s.parameterize.downcase.gsub("_","")})
      return unless (value = @raw_row[key]).present?
      return unless (work_id = find_work_id_from_unique_metadata(key, value))
    end
    proxy.update(work_id: work_id)
  end

  def find_work_id_from_unique_metadata field_name, value
    field_solr_name = schema.get_field(field_name).solr_name
    query = "_query_:\"{!dismax qf=#{field_solr_name}}#{value}\""
    response = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path, params: { fq: query, rows: 1, start: 0})["response"]
    if response["numFound"] > 1
      report_error( :id_not_unique , "",  row_number: row_number, object_id: @proxy.id, options_name: field_name, option_values: value )
    end
    return response["docs"][0]["id"]
  end

  def setAdminSet 
    return if @metadata[:admin_set_id]
    asets = AdminSet.where({title: "Bulk Ingest Set"})
    asets = AdminSet.find('admin_set/default') if asets.blank?
    @metadata[:admin_set_id] = Array(asets).first.id unless asets.blank?
  end

  def setMetadataInheritance
    return if @metadata[:metadataInheritance].present?
    @metadata[:metadataInheritance] = @options["metadataInheritance"] unless @options["metadataInheritance"].blank?
  end

  def report_error type, message, **args
    puts "ERROR MESSAGE: #{message}"
    @proxy.update(status: "error", message: message)
    args[:type]=type
    (@parsing_errors ||= []) <<  BulkOps::Error.new(**args)
  end

  def find_field_name(field)
    operation.find_field_name(field)
  end

  def schema
    ScoobySnacks::METADATA_SCHEMA
  end


end
