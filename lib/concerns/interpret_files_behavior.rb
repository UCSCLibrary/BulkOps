module BulkOps::InterpretFilesBehavior
  extend ActiveSupport::Concern


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

  private 

  def get_removed_filesets(filestring)
    file_ids = BulkOps::Parser.split_values(filestring)
    file_ids.select{|file_id| BulkOps::SolrService.record_exists?(file_id)}

# This part handles filenames in addition to file ids. It doesn't work yet!
#    file_ids.map do |file_id| 
      # If the filename is the id of an existing record, keep that
#      next(file_id) if (BulkOps::SolrService.record_exists?(file_id))
      # If this is the label (i.e.filename) of an existing fileset, use that fileset id
      # TODO MAKE THIS WORK!!
#      next(filename) if (filename_exists?(filename))
#      File.join(BulkOps::INGEST_MEDIA_PATH, filename_prefix, filename)
#    end
  end
  
  def delete_file_set fileset_id
    BulkOps::DeleteFileSetJob.perform_later(fileset_id, operation.user.email )
  end


end
