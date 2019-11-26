class BulkOps::Parser
  require 'uri'

  attr_accessor :proxy, :raw_data, :raw_row

  delegate :relationships, :operation, :row_number, :work_id, :visibility, :work_type, :reference_identifier, :order, to: :proxy

  def self.is_file_set? metadata, row_number
    return false unless metadata[row_number].present?
    # If the work type is explicitly specified, use that
    if (type_key = metadata.keys.find{|key| key.downcase.gsub(/[_\-\s]/,"").include?("worktype") })
      return true if metadata[type_key].downcase == "fileset" 
      return false if metadata[type_key].present?
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

  def initialize prx, metadata_sheet=nil
    @proxy = prx
    @raw_data = (metadata_sheet || proxy.operation.metadata)
    @raw_row = @raw_data[@proxy.row_number]
    @metadata = {}
    @parsing_errors = []
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
    interpret_file_fields
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
      # separate values in identical columns using the separator
      row[header] = (Array(row[header]) << value).join(BulkOps::SEPARATOR)
    end
    @raw_row = row
  end

  def connect_existing_work
    return unless (column_name = operation.options["update_identifier"])
    return unless (key = @raw_row.keys.find{|key| key.to_s.parameterize.downcase.gsub("_","") == column_name.to_s.parameterize.downcase.gsub("_","")})
    return unless (value = @raw_row[key])
    return unless (work_id = find_work_id_from_unique_metadata(key, value)) 
    proxy.update(work_id: work_id)
  end

  def find_work_id_from_unique_metadata field_name, value
    field_solr_name = schema.get_field(field_name).solr_name
    query = "_query_:\"{!raw f=#{field_name}}#{value}\""
    response = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path, params: { fq: query, rows: 1, start: 0})["response"]
    return response["docs"][0]["id"]
  end

  def interpret_controlled_fields 
    
    # The labels array tracks the contents of columns marked as labels,
    # which may require special validation
    labels = {}

    # This hash is populated with relevant data as we loop through the fields
    controlled_data = {}

    @raw_row.each do |field_name, value| 
      next if value.blank?  or field_name.blank?
      field_name = field_name.to_s

      #If our CSV interpreter is feeding us the headers as a line, ignore it.
      next if field_name == value

      #check if they are using the 'field_name.authority' syntax
      authority = nil
      if ((split=field_name.split('.')).count == 2)
        authority = split.last 
        field_name = split.first
      end

      # get the field name, if this column is a metadata field
      field_name_norm = find_field_name(field_name)
      field = schema.get_field(field_name_norm)

      # Ignore anything that isn't a controlled field
      next unless field.present? && field.controlled?

      # Keep track of label fields
      if field_name.downcase.ends_with?("label")
        next if operation.options["ignore_labels"]  
        labels[field_name_norm] ||= []
        labels[field_name_norm] += split_values value
        next unless operation.options["import_labels"]
      end

      remove = field_name.downcase.starts_with?("remove") || field_name.downcase.starts_with?("delete")
      
      # handle multiple values
      value_array = split_values(value)
      controlled_data[field_name_norm] ||= [] unless value_array.blank?
      value_array.each do |value|
        # Decide of we're dealing with a label or url
        # It's an ID if it's a URL and the name doesn't end in 'label'
        value.strip!
        if value =~ /^#{URI::regexp}$/ and !field_name.downcase.ends_with?("label")
          value_id = value
        #          label = WorkIndexer.fetch_remote_label(value)
        #          error_message =  "cannot fetch remote label for url: #{value}"
        #          report_error( :cannot_retrieve_label , error_message, url: value, row_number: row_number) unless label
        else
          # It's a label, so unescape it and get the id
          value = unescape_csv(value)
          value_id = get_remote_id(value, property: field_name_norm, authority: authority) || localAuthUrl(field_name_norm, value)
          #          label = value
          report_error(:cannot_retrieve_url, 
                       message: "cannot find or create url for controlled vocabulary label: #{value}", 
                       url: value, 
                       row_number: row_number) unless value_id
        end
        controlled_data[field_name_norm] << {id: value_id, remove: field_name.downcase.starts_with?("remove")}
      end
    end

    # Actually add all the data
    controlled_data.each do |property_name, data|
      @metadata["#{property_name}_attributes"] ||= [] unless data.blank?
      data.uniq.each do |datum| 
        atts = {"id" => datum[:id]}
        atts["_delete"] = true if datum[:remove]
        @metadata["#{property_name}_attributes"] << atts
      end
    end
  end

  def interpret_scalar_fields
     @raw_row.each do |field, values| 
      next if values.blank? or field.nil? or field == values
      # get the field name, if this column is a metadata field
      next unless field_name = find_field_name(field.to_s)
      field = schema.get_field(field_name)
      # Ignore controlled fields
      next if field.controlled?
      split_values(values).each do |value|
        next if value.blank?
        value = value.strip.encode('utf-8', :invalid => :replace, :undef => :replace, :replace => '_') unless value.blank?
        value = unescape_csv(value)
        (@metadata[field_name] ||= []) << value
       end
    end
   end

  def interpret_file_fields
    # This method handles file additions and deletions from the spreadsheet
    # if additional files need to be deleted because the update is set to replace
    # some or all existing files, those replacement-related deletions are handled
    # by the BulkOps::Operation.
    #

    @raw_row.each do |field, value|
      next if value.blank?  or field.blank?
      field = field.to_s
      #If our CSV interpreter is feeding us the headers as a line, ignore it.
      next if field == value

      # Check if this is a file field, and whether we are removing or adding a file
      next unless (action = BulkOps::Verification.is_file_field?(field))
      
      # Move on if this field is the name of another property (e.g. masterFilename)
      next if find_field_name(field)
      
      # Check if we are removing a file
      if action == "remove"
        get_removed_filesets(value).each { |fileset_id| delete_file_set(file_set_id) } 
      else
        # Add a file
        operation.get_file_paths(value).each do |filepath|
          begin
            uploaded_file = Hyrax::UploadedFile.create(file:  File.open(filepath), user: operation.user)
            (@metadata[:uploaded_files] ||= []) << uploaded_file.id unless uploaded_file.id.nil?
          rescue Exception => e  
            report_error(:upload_error,
                         message: "Error opening file: #{ filepath } -- #{e}",
                         file: File.join(BulkOps::INGEST_MEDIA_PATH,filename),
                         row_number: row_number)
          end
        end
      end

      # Check if any of the upcoming rows are child filesets
      i = 1
      while self.class.is_file_set?(@metadata,row_number+i)
        child_row.each do |field,value|
          next if value.blank?
          title = value if ["title","label"].include?(field.downcase.strip)
          if BulkOps::Verification.is_file_field?(field)
            operation.get_file_paths(value).each do |filepath|
              uploaded_file = Hyrax::UploadedFile.create(file:  File.open(filepath), user: operation.user)
            end
          end
        end
        i+=1
      end

    end
  end

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

  def interpret_relationship_fields
    @raw_row.each do |field,value|
      next if value.blank?  or field.blank?
      field = field.to_s              
      value = unescape_csv(value)
      identifer_type = reference_identifier

      next if value == field

      # Correctly interpret the notation "parent:id", "parent id" etc in a column header
      if (split = field.split(/[:_\-\s]/)).count == 2
        identifier_type = split.last
        relationship_type = split.first.to_s
      else
        relationship_type = field
      end

      relationship_type = self.class.normalize_relationship_field_name(relationship_type)
      case relationship_type
      when "order"
        # If the field specifies the object's order among siblings 
        @proxy.update(order: value.to_f)
        next
      when "collection"
        # If the field specifies the name or ID of a collection,
        # find or create the collection and update the metadata to match
        col = find_or_create_collection(value)
        ( @metadata[:member_of_collection_ids] ||= [] ) << col.id if col
        next
      when "parent", "child"
        
        # correctly interpret the notation "id:a78C2d81"
        identifier_type, object_identifier = interpret_relationship_value(identifier_type, value)
        
        relationship_parameters =  { work_proxy_id: @proxy.id,
                                     identifier_type: identifier_type,
                                     relationship_type: relationship_type,
                                     object_identifier: object_identifier,
                                     status: "new"}
        
        #add previous sibling link if necessary
        previous_value = @raw_data[row_number-1][field]
        # Check if this is a parent relationship, and the previous row also has one
        if previous_value.present? && (relationship_type == "parent")
          # Check if the previous row has the same parent as this row
          if object_identifier == interpret_relationship_value(identifier_type, previous_value, field).last
            # If so, set the previous sibling parameter on the relationshp 
            #    to the id for the proxy associated with the previous row
            relationship_parameters[:previous_sibling] = operation.work_proxies.find_by(row_number: row_number-1).id 
          end
        end
        BulkOps::Relationship.create(relationship_parameters)
      end
    end
  end

  def self.normalize_relationship_field_name field
    normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
    BulkOps::RELATIONSHIP_FIELDS.find{|rel_field| normfield == rel_field }
  end

  def find_previous_parent field="parent"
    #Return the row number of the most recent preceding row that does
    # not itself have a parent defined
    i = 1;
    while (prev_row = raw_data[row_number - i])
      return (row_number - i) if prev_row[field].blank?
      i += 1
    end
  end

  def interpret_relationship_value id_type, value, field="parent"
    #Handle "id:20kj4259" syntax if it hasn't already been handled
    if (split = value.to_s.split(":")).count == 2
      id_type, value = split.first
      value = split.last
    end
    #Handle special shorthand syntax for refering to relative row numbers
    if id_type == "row"
      #if the value is an integer
      if value =~ /\A[-+]?[0-9]+\z/
        if value.to_i < 0
        # if given a negative integer, count backwards from the current row (remember that value.to_i is negative)
          return [id_type,row_number + value.to_i]
        elsif value.to_i > 0
          # if given a positive integer, remove the row offset
          value = (value.to_i - BulkOps::ROW_OFFSET).to_s
        end
      elsif value.to_s.downcase.include?("prev")
        # if given any variation of the word "previous", get the first preceding row with no parent of its own
        return [id_type,find_previous_parent(field)]
      end
    end
    return [id_type,value]
  end

  def unescape_csv(value)
    value.gsub(/\\(['";,])/,'\1')
  end


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


  def mintLocalAuthUrl(auth_name, value)
    value.strip!
    id = value.parameterize
    auth = Qa::LocalAuthority.find_or_create_by(name: auth_name)
    entry = Qa::LocalAuthorityEntry.create(local_authority: auth,
                                           label: value,
                                           uri: id)
    return localIdToUrl(id,auth_name)
  end

  def findAuthUrl(auth, value)
    value.strip!
    return nil if auth.nil?
    return nil unless (entries = Qa::Authorities::Local.subauthority_for(auth).search(value))
    entries.each do |entry|
      #require exact match
      next unless entry["label"].force_encoding('UTF-8') == value.force_encoding('UTF-8')
      url = entry["url"] || entry["id"]
#      url = localIdToUrl(url,auth) unless url =~ URI::regexp
      return url
    end
    return nil
  end

  def localIdToUrl(id,auth_name) 
    root_urls = {production: "https://digitalcollections.library.ucsc.edu",
                 staging: "http://digitalcollections-staging.library.ucsc.edu",
                 development: "http://#{Socket.gethostname}",
                 test: "http://#{Socket.gethostname}"}
    return "#{root_urls[Rails.env.to_sym]}/authorities/show/local/#{auth_name}/#{id}"
  end

  def getLocalAuth(field_name)
    field =  schema.get_property(field_name)
    # There is only ever one local authority per field, so just pick the first you find
    if vocs = field.vocabularies
      vocs.each do |voc|
        return voc["subauthority"] if voc["authority"].downcase == "local"
      end
    end
    return nil
  end

  def setAdminSet 
    return if @metadata[:admin_set_id]
    asets = AdminSet.where({title: "Bulk Ingest Set"})
    asets = AdminSet.find('admin_set/default') if asets.blank?
    @metadata[:admin_set_id] = Array(asets).first.id unless asets.blank?
  end

  def setMetadataInheritance
    return if @metadata[:metadataInheritance].present?
    @metadata[:metadataInheritance] = operation.options["metadataInheritance"] unless operation.options["metadataInheritance"].blank?
  end

  def report_error type, message, **args
    puts "ERROR MESSAGE: #{message}"
    @proxy.update(status: "error", message: message)
    args[:type]=type
    (@parsing_errors ||= []) <<  BulkOps::Error.new(**args)
  end

  def get_removed_filesets(filestring)
    file_ids = split_values(filestring)
    file_ids.select{|file_id| record_exists?(file_id)}

# This part handles filenames in addition to file ids. It doesn't work yet!
#    file_ids.map do |file_id| 
      # If the filename is the id of an existing record, keep that
#      next(file_id) if (record_exists?(file_id))
      # If this is the label (i.e.filename) of an existing fileset, use that fileset id
      # TODO MAKE THIS WORK!!
#      next(filename) if (filename_exists?(filename))
#      File.join(BulkOps::INGEST_MEDIA_PATH, filename_prefix, filename)
#    end
  end

  def delete_file_set fileset_id
    BulkOps::DeleteFileSetJob.perform_later(fileset_id, operation.user.email )
  end
 
  def record_exists? id
    operation.record_exists? id
  end

  def localAuthUrl(property, value) 
    return value if (auth = getLocalAuth(property)).nil?
    url =   findAuthUrl(auth, value) ||  mintLocalAuthUrl(auth,value)
    return url
  end

  def find_collection(collection)
    cols = Collection.where(id: collection)
    cols += Collection.where(title: collection).select{|col| col.title.first == collection}
    return cols.last unless cols.empty?
    return false
  end

  def find_or_create_collection(collection)
    col = find_collection(collection)
    return col if col
    return false if collection.to_i > 0
    col = Collection.create(title: [collection.to_s], depositor: operation.user.email, collection_type: Hyrax::CollectionType.find_by(title:"User Collection"))
  end

  def get_remote_id(value, authority: nil, property: nil)
    return false
    #TODO retrieve URL for this value from the specified remote authr
  end

  def format_param_name(name)
    name.titleize.gsub(/\s+/, "").camelcase(:lower)
  end

  def schema
    ScoobySnacks::METADATA_SCHEMA
  end

  def find_field_name(field)
    operation.find_field_name(field)
  end

  def downcase_first_letter(str)
    return "" unless str
    str[0].downcase + str[1..-1]
  end

  def split_values value_string
    # Split values on all un-escaped separator character (escape character is '\')
    # Then replace all escaped separator charactors with un-escaped versions
    value_string.split(/(?<!\\)#{BulkOps::SEPARATOR}/).map{|val| val.gsub("\\#{BulkOps::SEPARATOR}",BulkOps::SEPARATOR).strip}
  end

end
