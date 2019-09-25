class BulkOps::WorkProxy < ActiveRecord::Base

  require 'uri'
  OPTION_FIELDS = ['visibility','work type']
  RELATIONSHIP_FIELDS = ['parent','child','collection','order']
  REFERENCE_IDENTIFIER_FIELDS = ['Reference Identifier','ref_id','Reference ID','Relationship ID','Relationship Identifier','Reference Identifier Type','Reference ID Type','Ref ID Type','relationship_identifier_type','relationship_id_type']
  FILE_FIELDS = ['file','files','filename','filenames']
  FILE_ACTIONS = ['add','upload','remove','delete']
  SEPARATOR = ';'
  self.table_name = "bulk_ops_work_proxies"
  belongs_to :operation, class_name: "BulkOps::Operation", foreign_key: "operation_id"
  has_many :relationships, class_name: "BulkOps::Relationship"

  attr_accessor :proxy_errors

  def initialize *args
    super *args
    place_hold if @work_id
  end

  def work
    return @work if @work
    begin
      @work = ActiveFedora::Base.find(work_id)
    rescue 
      return false
    end
    return @work
  end

  def work_type
    super || operation.work_type || "Work"
  end

  def place_hold
    # TODO make it so nobody can edit the work
  end

  def lift_hold
    # TODO make it so people can edit the work again
  end

  def interpret_data raw_data
    admin_set = AdminSet.where(title: "Bulk Ingest Set").first || AdminSet.find(AdminSet.find_or_create_default_admin_set_id)
    metadata = {admin_set_id: admin_set.id}
    metadata.merge! interpret_file_fields(raw_data)
    metadata.merge! interpret_controlled_fields(raw_data)
    metadata.merge! interpret_scalar_fields(raw_data)
    metadata.merge! interpret_relationship_fields(raw_data )
    metadata.merge! interpret_option_fields(raw_data)
    metadata = setAdminSet(metadata)
    metadata = setMetadataInheritance(metadata)
    return metadata
  end

  def proxy_errors
    @proxy_errors ||= []
  end

  private 

  def is_file_field? field
    operation.is_file_field? field
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
    value_string.split(/(?<!\\)#{SEPARATOR}/).map{|val| val.gsub("\\#{SEPARATOR}",SEPARATOR).strip}
  end

  def interpret_controlled_fields raw_data
    
    # The labels array tracks the contents of columns marked as labels,
    # which may require special validation
    labels = {}

    # This hash is populated with relevant data as we loop through the fields
    controlled_data = {}

    raw_data.each do |field_name, value| 
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
          id = value
#          label = WorkIndexer.fetch_remote_label(value)
#          error_message =  "cannot fetch remote label for url: #{value}"
#          report_error( :cannot_retrieve_label , error_message, url: value, row_number: row_number) unless label
        else
          # It's a label, so unescape it and get the id
          value = unescape_csv(value)
          id = get_remote_id(value, property: field_name_norm, authority: authority) || localAuthUrl(field_name_norm, value)
#          label = value
          report_error(:cannot_retrieve_url, 
                       message: "cannot find or create url for controlled vocabulary label: #{value}", 
                       url: value, 
                       row_number: row_number) unless id
        end
        controlled_data[field_name_norm] << {id: id, remove: field_name.downcase.starts_with?("remove")}
      end
    end
    
    #delete any duplicates (if someone listed a url and also its label, or the same url twice)
    controlled_data.each{|field_name, values| controlled_data[field_name] = values.uniq }
        
    # Actually add all the data
    metadata = {}
    leftover_data = raw_data.dup.to_hash
    controlled_data.each do |property_name, data|
      metadata["#{property_name}_attributes"] ||= [] unless data.blank?
      data.each do |datum| 
        atts = {"id" => datum[:id]}
        atts["_delete"] = true if datum[:remove]
        metadata["#{property_name}_attributes"] << atts
        leftover_data.except! property_name
      end
    end
    #return [metadata, leftover_data]
    return metadata
  end

  def interpret_scalar_fields raw_data
    metadata = {}
    raw_data.each do |field, values| 
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
        (metadata[field_name] ||= []) << value
      end
    end
    return metadata
  end

  def interpret_file_fields raw_data
    # This method handles file additions and deletions from the spreadsheet
    # if additional files need to be deleted because the update is set to replace
    # some or all existing files, those replacement-related deletions are handled
    # by the BulkOps::Operation.
    #
    # TODO: THIS DOES NOT YET MANAGE THE ORDER OF INGESTED FILESETS
    
    metadata = {}
    raw_data.each do |field, value|
      next if value.blank?  or field.blank?
      field = field.to_s
      #If our CSV interpreter is feeding us the headers as a line, ignore it.
      next if field == value


      # Check if this is a file field, and whether we are removing or adding a file
      next unless (action = is_file_field?(field))
      
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
            (metadata[:uploaded_files] ||= []) << uploaded_file.id unless uploaded_file.id.nil?
          rescue Exception => e  
            report_error(:upload_error,
                         message: "Error opening file: #{ filepath } -- #{e}",
                         file: File.join(BulkOps::Operation::INGEST_MEDIA_PATH,filename),
                         row_number: row_number)
          end
        end
      end
    end
    return metadata
  end

  def interpret_option_fields raw_data
    raw_data.each do |field,value|
      next if value.blank? or field.blank?
      field = field.to_s
      next if value == field

      normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
      if ["visibility", "public"].include?(normfield)
        update(visibility: format_visibility(value))
      end
      if ["worktype","model","type"].include?(normfield)
        update(work_type: format_worktype(value) )
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
        update(reference_identifier: format_reference_id(value))
      end
    end
    return {}
  end

  def interpret_relationship_fields raw_data, row_number=nil
    metadata = {}
    raw_data do |field,value|
      next if value.blank?  or field.blank?
      field = field.to_s              
      value = unescape_csv(value)
      identifer_type = reference_identifier

      next if value == field

      if (split = field.split(":")).count == 2
        identifier_type = split.last
        field = split.first.to_s
      end

      relationship_type = normalize_relationship_field_name(field)
      case relationship_type
      when "order"
         # If the field specifies the object's order among siblings (usually for multiple filesets)
        update(order: value.to_f)
      when "collection"
        # If the field specifies the name or ID of a collection,
        # find or create the collection and update the metadata to match
        col = find_or_create_collection(value)
        ( metadata[:member_of_collection_ids] ||= [] ) << col.id if col
      when "parent", "child"
        object_id = interpret_relationship_value(value,identifier_type)
      end
      
      # correctly interpret the notation "id:a78C2d81"
      if ((split = value.split(":")).count == 2)
        identifier_type = split.first
        value = split.last
      end

      interpret_relationship_value(identifier_type, value)

      relationship_parameters =  { work_proxy_id: id,
                                      identifier_type: ref_type,
                                      relationship_type: normfield,
                                      object_identifier: value,
                                      status: "new"}

      #add previous sibling link if necessary
      previous_value = op.metadata[row_number-1][field]
      if previous_value.present? && (ref_type == "parent")
        if value == interpret_relationship_value(ref_type, previous_value)
          relationship_parameters[:previous_sibling] = operation.work_proxies.find_by(row_number: row_number-1).id 
        end
      end
      BulkOps::Relationship.create(relationship_parameters)
    end
    return metadata
  end

  def normalize_relationship_field_name field
    normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
    RELATIONSHIP_FIELDS.find{|field| normfield.include?(field) }
  end

  def find_previous_parent field
    i = 0;
    while (prev_row = operation.metadata[row_number - i])
      return (row_number - i) if prev_row[field].blank?
    end
  end

  def find_previous_sibling (field,ref_type,object)
    previous_value = interpret_relationship_value(ref_type,op.metadata[row_number-1][field])
    return nil unless previous_value == object
    return 
  end

  def interpret_relationship_value id_type, value, field="parent"
    #Handle "id:20kj4259" syntax if it hasn't already been handled
    if (split = value.to_s.split(":")).count == 2
      id_type = split.first
      value = split.last
    end
    #Handle special shorthand syntax for refering to relative row numbers
    if id_type == "row"
      if value.to_i < 0
        # if given a negative integer, count backwards from the current row 
        return row_number - value
      elsif value.to_s.downcase.include?("prev")
        # if given any variation of the word "previous", get the first preceding row with no parent of its own
        return find_previous_parent(field)
      end
    end
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
    return type ||= operation.work_type || "Work"
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

  def setAdminSet metadata
    return metadata if metadata[:admin_set_id]
    asets = AdminSet.where({title: "Bulk Ingest Set"})
    asets = AdminSet.find('admin_set/default') if asets.blank?
    metadata[:admin_set_id] = Array(asets).first.id unless asets.blank?
    return metadata
  end

  def setMetadataInheritance metadata
    return metadata if metadata[:metadataInheritance].present?
    metadata[:metadataInheritance] = operation.options["metadataInheritance"] unless operation.options["metadataInheritance"].blank?
    return metadata
  end

  def report_error type, message, **args
    puts "ERROR MESSAGE: #{message}"
    update(status: "error", message: message)
    args[:type]=type
    (@proxy_errors ||= []) <<  BulkOps::Error.new(**args)
  end

  def filename_prefix
    @filename_prefix ||= operation.filename_prefix
  end

  def record_exists?
    operation.record_exists? work_id
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
#      File.join(BulkOps::Operation::INGEST_MEDIA_PATH, filename_prefix, filename)
#    end
  end

  def delete_file_set fileset_id
    BulkOps::DeleteFileSetJob.perform_later(fileset_id, operation.user.email )
  end
  
end
