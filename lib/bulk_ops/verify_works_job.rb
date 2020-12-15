class VerifyWorksJob < Hyrax::ApplicationJob

  NOTIFICATION_RECIPIENTS = ['ethenry@ucsc.edu']
  
  def perform(job_name: nil, collection_ids: nil, log: nil, report: nil, ignore_private_works: false)
    @job_name = job_name || Time.now.strftime("%m-%d-%Y_%I%M%p")
    
    recover_existing_data
    @log_file ||= File.open(logname(),'w')
    @data = {}
    
    begin
    
    rows = 200

    if collection_ids.blank?
      query = ActiveFedora::SolrQueryBuilder.construct_query_for_rel("has_model" => "Collection")
      collection_ids = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path, params: { fq: query, rows: 999})["response"]["docs"].map{|doc| doc["id"]}
    end

    log("starting verification scan for #{collection_ids.count} collections")

    collection_ids.each do |collection_id|
      start = 0
      collection = SolrDocument.find(collection_id)

      verify_collection(collection) unless (collection.visibility == "restricted" && ignore_private_works)
      
      query = ActiveFedora::SolrQueryBuilder.construct_query_for_rel("has_model" => "Work","member_of_collection_ids" => collection_id.to_s)
      num_works = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path, params: { fq: query, rows: 0})["response"]["numFound"]
      log("verifying #{num_works} members of collection: #{collection.title.first} (#{collection.id})")
      until start > num_works
        works = ActiveFedora::SolrService.instance.conn.get(ActiveFedora::SolrService.select_path, params: { fq: query, rows: rows, start: start})["response"]["docs"]
        works.each do  |wrk|
          verify_work(wrk["id"]) unless (work.visibility == "restricted" && ignore_private_works)
        end
        start = start + rows
        write_data
      end
      write_data
    end

    ensure
      write_data
      @log_file.close unless @log_file.nil?
    end
    generate_report
    File.delete(logname("in_progress_data")) if File.exist?(logname("in_progress_data"))
  end

  def write_data
    File.open(logname("in_progress_data"),'w') do |data_file|
      data_file.write(@data.to_json)
    end
  end

  def interpret_error_code(code)
    bin_code = code.to_s(2).reverse
    tests.reduce([]){ |errors, test| bin_code[test[:id]].to_i.zero? ? errors : (errors << test[:message]) }
  end

  def generate_report
    @codes = @data.keys.group_by{|key| @data[key]}
    report_filename = logname("final_report")
    File.open(report_filename,'w') do |data_file|
      @codes.each do |code, ids|
        next if code.to_i == 0
        data_file.puts("We found #{ids.count} objects with the following combination of errors:")
        interpret_error_code(code).each{ |message| data_file.puts(message)}
        data_file << "\n"
        data_file.puts("These are the ids of the works with this combination of errors:")
        data_file.puts(ids.join(','))
        data_file << "\n------------------------------------------\n"
      end
    end
    send_report_data report_filename
  end

  def send_report_data report_filename
    subject = "DAMS content scan results ready"
    message = "We finished running checks on digital objects in the DAMS.\n"
    report = File.read(report_filename || logname("final_report"))
    message += "We found the following issues:\n\n#{report}" unless report.blank?
    NOTIFICATION_RECIPIENTS.each do |email|
      ActionMailer::Base.mail(from: "admin@digitalcollections.library.ucsc.edu",
                              to: email,
                              subject: subject,
                              body: message).deliver
    end
  end
  
  def recover_existing_data
    return unless File.exists?(logname("in_progress_data"))
    @data = JSON.parse(File.read(logname("in_progress_data"))) || {}
  end

  def verify_collection(doc)
    doc = SolrDocument.find(doc) if doc.is_a?(String)
    #skip if collection is included in existing data
    return if @data[doc.id].present?
    log("verifying collection: #{doc.title.first} (#{doc.id})")
    run_tests(doc)
  end

  def logname(log_id="log")
    File.join(Rails.root,"log","verification_#{log_id}_#{@job_name}.txt")
  end

  def verify_work(doc)
    #skip if work is included in existing data
    doc = SolrDocument.find(doc) if doc.is_a?(String)
    return if @data[doc.id].present?
    log("verifying work: #{doc.title.first} (#{doc.id})")
    run_tests(doc)
  end

  def log(line)
    @log_file.puts(Time.now.strftime("%m/%d/%Y %I:%M %p: ")+line)
  end


  def tests
    [{id: 1, method: :page_load, type: :display,
       message: "The work show page failed to load successfully"},
     {id: 2, method: :simple_metadata_display, type: :display,
       message: "Some simple metadata is not displaying properly"},
     {id: 3, method: :simple_metadata_indexing,
       message: "Some simple metadata is not being indexed properly"},
     {id: 4, method: :controlled_metadata_display, type: :display,
       message: "Some controlled metadata is not displaying properly"},
     {id: 5, method: :controlled_metadata_indexing,
       message: "Some controlled metadata is not being indexed properly"},
     {id: 6, method: :character_encoding,
       message: "There are some character encoding issues with some metadata"},
     {id: 7, method: :collection_indexing,
       message: "This object is not indexing its parent collection properly"},
     {id: 8, method: :collection_display, type: :display,
       message: "The object is not displaying its parent collection properly"},
     {id: 9, method: :parent_indexing, object_types: [Work],
       message: "The object is not indexing its parent work properly"},
     {id: 10, method: :primary_image_indexing, object_types: [Work],
       message: "The object is not indexing its primary image properly"},
     {id: 11, method: :primary_image_display, type: :display, object_types: [Work],
       message: "The object is not displaying its primary image properly"},
     {id: 12, method: :ordered_members_indexing, object_types: [Work],
       message: "The object is not indexing its ordered members properly"},
     {id: 13, method: :child_work_display, type: :display, object_types: [Work],
       message: "The object is not displaying its child works properly"},
     {id: 14, method: :child_work_indexing, object_types: [Work],
       message: "The object is not indexing its child works properly"},
     {id: 15, method: :fileset_indexing, object_types: [Work],
       message: "The object is not indexing its filesets properly"},
     {id: 16, method: :inheritance,
      message: "The object has not inherited metadata properly from its parent work"},
    {id: 17, method: :fileset_permissions,
      message: "The visibility of this work's fileset(s) is more restrictive than that of the work itself"}]
  end

  def get_test id
    tests.find{|test| test.id == id}
  end

  def run_tests doc
    doc = SolrDocument.find(doc) if doc.is_a?(String)
    @doc = doc
    @object = ActiveFedora::Base.find(doc.id)
    # this code is a binary combination of the ids of all the failed tests
    result_code = tests.select{|test| !run_test(test) }.reduce(0){ |code, test| code + 2**test[:id].to_i}
    @data[doc.id]=result_code
  end

  def run_test test
    test = get_test test if test.is_a?(Integer)
    return true if (test[:object_types].present? && !test[:object_types].include?(@Object.class))
    return false unless (@doc.id == @object.id)
    begin
      return send(test[:method])
    rescue
      return false
    end
  end

  def object_exists id
    @doc.present? && @object.present?
  end

  def page_load
    return true
  end

  def simple_metadata_display
    return true
  end

  def schema
    @schema ||= ScoobySnacks::METADATA_SCHEMA
  end
  
  def simple_metadata_indexing
    simple_metadata = schema.all_field_names - schema.controlled_field_names - schema.inheritable_field_names
    simple_metadata.all?{|field_name| @doc.send(field_name).to_a.sort == @object.send(field_name).to_a.sort}
  end
  
  def controlled_metadata_display
    return true
  end

  def controlled_metadata_indexing
    controlled_metadata = schema.controlled_field_names
    controlled_metadata.all? do |field_name|
      @object.send(field_name).map{|rel| @doc.send(field_name).include?(WorkIndexer.fetch_remote_label(rel.id))}
    end
  end

  def character_encoding
    return true
    #how do I test for this kind of error exactly?
  end

  def collection_indexing
    @doc.member_of_collection_ids.sort == @object.member_of_collection_ids.sort
  end

  def collection_display
    return true
  end

  def parent_indexing
    if @doc.parent_id.present?
      parent = SolrDocument.find(@doc.parent_id)
      return false unless (parent.ordered_member_ids & parent.member_ids & parent.member_work_ids).include?(@doc.id)
    end
    return true
  end

  def primary_image_indexing
    return true unless @doc.image?
    begin
      return SolrDocument.find(representative_id).image?
    rescue
      return false
    end
  end

  def primary_image_display
    return true
  end

  def ordered_members_indexing
    return false unless  @object.ordered_member_ids == @object.file_set_ids + @object.member_work_ids
    return false unless @doc.ordered_member_ids == @object.ordered_member_ids
    return true
  end

  def child_work_indexing
    @object.member_work_ids.each do |id|
      return false unless SolrDocument.find(id).parent_id == @doc.id
    end
    return true
  end

  def child_work_display
    return true
  end

  def multiple_filesets_indexing
    return true
  end
  
  def inheritance
    return true unless (parent_ids = (Array(@doc.parent_id) + Array(@doc.member_of_collection_ids))).present?
    parent_ids.each do |parent_id|
      parent_doc = SolrDocument.find(parent_id)
      schema.inheritable_field_names.each do |field_name|
        return false if parent_doc.send(field_name).present? && @doc.send(field_name).blank?
      end
    end
    return true
  end

  def fileset_permissions
    return true unless (file_set_ids = doc.file_set_ids).present?
    return true if @doc.visibility == "restricted"
    !file_set_ids.any?{|id| SolrDocument.find(id).visibility == "restricted"}
  end

end
