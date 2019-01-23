class BulkOps::WorkProxy < ActiveRecord::Base
  require 'uri'
  OPTION_FIELDS = ['visibility','work type']
  RELATIONSHIP_FIELDS = ['parent','child','collection','next','order']
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
    metadata = {}
    metadata.merge! interpret_file_fields(raw_data)
    metadata.merge! interpret_controlled_fields(raw_data)
    metadata.merge! interpret_scalar_fields(raw_data)
    metadata.merge! interpret_relationship_fields(raw_data )
    metadata.merge! interpret_option_fields(raw_data)
    return metadata
  end

  private 

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
    col = Collection.create(title: [collection.to_s])
  end

  def get_remote_id(value, authority: nil, property: nil)
    return false
    #TODO retrieve URL for this value from the specified remote authr
  end

  def format_param_name(name)
    name.titleize.gsub(/\s+/, "").camelcase(:lower)
  end

  def schema
    ScoobySnacks::METADATA_SCHEMA["work_types"][work_type.downcase]
  end

  def find_field_name field
    field.gsub!(/[_\s-]?[lL]abel\z/,"") if field.downcase.ends_with? "label"
    field.gsub!(/\A[rR]emove[_\s-]?/,"") if field.downcase.starts_with? "remove"
    field.gsub!(/\A[rR]emove[_\s-]?/,"") if field.downcase.starts_with? "delete"

    field_name = nil
    field_name_options = [field, 
                          downcase_first_letter(field.parameterize.underscore.camelize), 
                          field.parameterize.underscore]
    field_name_options.each {|fld| field_name = fld if schema["properties"].keys.include?(fld)}
    return field_name || false
  end

  def downcase_first_letter(str)
    str[0].downcase + str[1..-1]
  end

  def interpret_controlled_fields raw_data
    
    # The labels array tracks the contents of columns marked as labels,
    # which may require special validation
    labels = {}

    # This hash is populated with relevant data as we loop through the fields
    controlled_data = {}

    raw_data.each do |field, value| 
      field = field.to_s

      #If our CSV interpreter is feeding us the headers as a line, ignore it.
      next if field == value

      #check if they are using the 'field_name.authority' syntax
      authority = nil
      if ((split=field.split('.')).count == 2)
        authority = split.last 
        field = split.first
      end

      # get the field name, if this column is a metadata field
      field_name = find_field_name(field)

      # Ignore anything that isn't a controlled field
      next unless schema["controlled"].include? field_name

      # Keep track of label fields
      if field.downcase.ends_with?("label")
        next if operation.options["ignore_labels"]  
        labels[field_name] ||= []
        labels[field_name] += value.split(SEPARATOR) 
        next unless operation.options["import_labels"]
      end

      remove = field.downcase.starts_with?("remove") || field.downcase.starts_with?("delete")
      
      # handle multiple values
      value.split(SEPARATOR).each do |value|
        
        # Decide of we're dealing with a label or url
        # It's an ID if it's a URL and the name doesn't end in 'label'
        if value =~ /^#{URI::regexp}$/ and !field.downcase.ends_with?("label")
          id = value
          label = WorkIndexer.fetch_remote_label(value)
          error_message =  "cannot fetch remote label for url: #{value}"
          report_error( :cannot_retrieve_label , error_message, url: value, row_number: row_number) unless label
        else
          # It's a label, so get the id
          id = get_remote_id(value, property: field_name, authority: authority) || localAuthUrl(field_name, value)
          label = value
          report_error(:cannot_retrieve_url, 
                       message: "cannot find or create url for controlled vocabulary label: #{value}", 
                       url: value, 
                       row_number: row_number) unless id
        end
        (controlled_data[field_name] ||= []) << {id: id, label: label, remove: field.downcase.starts_with?("remove")}
      end
    end
    
    #delete any duplicates (if someone listed a url and also its label, or the same url twice)
    controlled_data.each{|field, values| controlled_data[field] = values.uniq }
    
    if operation.options["compare_labels"]
      controlled_data.each do |field,values|
        unless labels['field'].count == values.count
          report_error(:mismatched_auth_terms, 
                       message: "Different numbers of labels and ids for #{field}",
                       row_number: row_number) 
        end
      end
      labels['field'].each do |label| 
        next if controlled_data.any?{|dt| dt["label"] == label}
        report_error(:mismatched_auth_terms, 
                     message: "There are controlled vocab term labels that no provided URL resolves to, in the field #{field}.",
                     row_number: row_number) 
      end
    end
    
    # Actually add all the data
    metadata = {}
    leftover_data = raw_data.dup
    controlled_data.each do |property_name, data|
      data.each do |datum| 
        atts = {"id" => datum[:id]}
        atts["_delete"] = true if datum[:remove]
        metadata["#{property_name}_attributes"] ||= []
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
      values.split(SEPARATOR).each do |value|
        field = field.to_s
        #If our CSV interpreter is feeding us the headers as a line, ignore it.
        next if field == value
        # get the field name, if this column is a metadata field
        next unless field_name = find_field_name(field)
        # Ignore controlled fields
        next if schema["controlled"].include? field_name
        (metadata[field_name] ||= []) << value.strip.encode('utf-8', :invalid => :replace, :undef => :replace, :replace => '_') unless value.blank?
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
      field = field.to_s
      #If our CSV interpreter is feeding us the headers as a line, ignore it.
      next if field == value
      # Move on if this field is the name of another property (e.g. masterFilename)
      next if find_field_name(field)
      # Ignore fields that aren't file fields
      field_parts = field.underscore.humanize.downcase.gsub("[-_]",' ').split(" ")
      next unless field_parts.any?{ |field_type| FILE_FIELDS.include?(field_type) }
      # Check if we are removing a file
      if (field_parts.any? {|action| ["remove","delete"].include?(action) })
        if SolrDocument.find(value)
          file_id = value 
        else
          work.file_sets.each{|fs| file_id = fs.id if (fs.label.include?(value) or value.include?(fs.label)) }
        end
        #Delete the file sometime later
        BulkOps::DeleteFileSetJob.perform_later(file_id, operation.user.email ) if file_id
      else
        # Add a file
        begin
          file = File.open(File.join(BulkOps::Operation::BASE_PATH,value))
          uploaded_file = Hyrax::UploadedFile.create(file: file, user: operation.user)
          (metadata[:uploaded_files] ||= []) << uploaded_file.id unless uploaded_file.id.nil?
        rescue Exception => e  
          report_error(:upload_error,
                       message: "Error opening file: #{File.join(BulkOps::Operation::BASE_PATH,value)} -- #{e}",
                       file: File.join(BulkOps::Operation::BASE_PATH,value),
                       row_number: row_number)
        end
      end
    end
    return metadata
  end

  def interpret_option_fields raw_data
    raw_data.each do |field,value|
      field = field.to_s
      next if value == field
      normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
      if ["visibility", "public"].include?(normfield)
        write_attribute(:visibility, format_visibility(value))
      end
      if ["worktype","model","type"].include?(normfield)
        write_attribute(:work_type,format_worktype(value) )
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
        reference_identifier = format_reference_id(value) 
      end
    end
    save
    self.save
    return {}
  end

  def interpret_relationship_fields raw_data
    metadata = {}
    raw_data.each do |field,value|
      field = field.to_s              
      if (split = field.split(":")).count == 2
        update(reference_identifier: split.first)
        field = split.last.to_s
      end

      next if value == field
      normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
      #      next unless RELATIONSHIP_FIELDS.include? normfield
      
      # If the field specifies the object's order among siblings (usually for multiple filesets)
      update(order: value.to_f) if normfield == "order"
      
      # If the field specifies the name or ID of a collection,
      # find or create the collection and update the metadata to match
      if ["collection","collectiontitle","memberofcollection","collectionname", "collectionid"].include?(normfield)
        col = find_or_create_collection(value)
        ( metadata[:member_of_collection_ids] ||= [] ) << col.id if col
      end

      # All variations of field names that require BulkOps::Relationship objects
      next unless ["parent","parentid","parentidentifier","parentwork","child","childid","childidentifier","childwork","next","nextfile","nextwork","nextid","nextfileidentifier","nextfileid","nextworkid"].include?(normfield)
      
      # find which type of relationship
      ["parent","child","next"].select{|type| normfield.include?(type)}.first
      # correctly interpret the notation "id:a78C2d81"
      if ((split = value.split(":")).count == 2)
        update(reference_identifier: split.first)
        value = split.last
      end
      BulkOps::Relationship.create( { work_proxy_id: id,
                                      identifier_type: reference_identifier,
                                      relationship_type: normfield,
                                      object_identifier: value,
                                      status: "new"} )
    end
    return metadata
  end

  def format_reference_id(value)
    return value if value=="id"
    value_method = value.titleize.gsub(/[-_\s]/,'').downcase_first_letter
    return value_method if (schema["properties"].keys.include?(value_method) || SolrDocument.new.respond_to?(value_method))
    case value.downcase.parameterize.gsub(/[_\s-]/,'')
    when "row", "rownum","row number"
      return "row"
    end
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
    id = value.parameterize
    auth = Qa::LocalAuthority.find_or_create_by(name: auth_name)
    entry = Qa::LocalAuthorityEntry.create(local_authority: auth,
                                           label: value,
                                           uri: id)
    return localIdToUrl(id,auth_name)
  end

  def findAuthUrl(auth, value)
    return nil if auth.nil?
    return nil unless (entries = Qa::Authorities::Local.subauthority_for(auth).search(value))
    entries.each do |entry|
      #require exact match
      if entry["label"] == value
        url = entry["url"]
        url ||= entry["id"]
        url = localIdToUrl(url,auth) unless url =~ URI::regexp
        return url
      end
    end
    return nil
  end

  def localIdToUrl(id,auth_name) 
    return "https://digitalcollections.library.ucsc.edu/authorities/show/local/#{auth_name}/#{id}"
  end

  def getLocalAuth(property)
    # There is only ever one local authority per field, so just pick the first you find
    if vocs = schema["properties"][property]["vocabularies"]
      vocs.each do |voc|
        return voc["subauthority"] if voc["authority"].downcase == "local"
      end
    elsif voc = schema["properties"][property]["vocabulary"]
      return voc["subauthority"] if voc["authority"].downcase == "local"
    end
    return nil
  end

  def setAdminSet metadata
    return metadata if metadata[:admin_set_id]
    asets = AdminSet.where({title: "Bulk Ingest Set"})
    asets = AdminSet.find('admin_set/default') if asets.blank?
    metadata[:admin_set_id] = asets.first.id unless asets.blank?
    return metadata
  end

  def report_error type, message, **args
    self.status = "error"
    self.message = message
    args[:type]=type
    puts "ERROR MESSAGE: #{message}"
    self.save
    (@proxy_errors ||= []) <<  BulkOps::Error.new(**args)
  end
end
