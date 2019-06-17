module BulkOps
  module Verification
    extend ActiveSupport::Concern
    
    def verify
      @verification_errors ||= []
      update(message: "verifying spreadsheet column headers")
      verify_column_headers
      update(message: "verifying controlled vocab urls (starting)")
      verify_remote_urls
      update(message: "verifying complex object structures")
      verify_internal_references
      update(message: "verifying that all files exist")
      verify_files
      verify_works_to_update if operation_type.to_s == "update"
      unless @verification_errors.blank?
        error_file_name = BulkOps::Error.write_errors!(@verification_errors, git)
        #notify everybody
        notify(subject: "Errors verifying bulk #{operation_type} in Hycruz", message: "Hyrax ran a verification step to make sure that the spreadsheet for this bulk #{operation_type} is formatted correctly and won't create any errors. We found some problems. You can see a summary of the issues at this url: https://github.com/#{git.repo}/blob/#{git.name}/#{git.name}/errors/#{error_file_name}. Please fix these problems and run this verification again. The bulk #{operation_type} will not be allowed to move forward until all verification issues are resolved.")
        return false
      end
      return true
    end

    def notify(subject: , message:)
      options["notifications"].each do |email|
        ActionMailer::Base.mail(from: "admin@digitalcollections.library.ucsc.edu",
                                to: email,
                                subject: subject,
                                body: message).deliver
      end
    end

    def is_file_field?(fieldname)
      return false if fieldname.blank?
      return false if schema.get_field(fieldname)
      field_parts = fieldname.underscore.humanize.downcase.gsub(/[-_]/,' ').split(" ")
      return false unless field_parts.any?{ |field_type| BulkOps::WorkProxy::FILE_FIELDS.include?(field_type) }
      return "remove" if field_parts.any?{ |field_type| ['remove','delete'].include?(field_type) }
      return "add"
    end

    def find_field_name(fieldname)
      name = fieldname.dup
      name.gsub!(/[_\s-]?[aA]ttributes$/,'')
      name.gsub!(/[_\s-]?[lL]abel$/,'')
      name.gsub!(/^[rR]emove[_\s-]?/,'')
      name.gsub!(/^[dD]elete[_\s-]?/,'')
      possible_fields = Work.attribute_names + schema.all_field_names
      matching_fields = possible_fields.select{|pfield| pfield.gsub(/[_\s-]/,'').parameterize == name.gsub(/[_\s-]/,'').parameterize }
      return false if matching_fields.blank?
      #      raise Exception "Ambiguous metadata fields!" if matching_fields.uniq.count > 1
      return matching_fields.first
    end

    def get_file_paths(filestring)
      return [] if filestring.blank?
      filenames = filestring.split(BulkOps::WorkProxy::SEPARATOR)
      filenames.map { |filename| File.join(BulkOps::Operation::INGEST_MEDIA_PATH, options['file_prefix'] || "", filename) }
    end

    def record_exists? id
      begin
        return true if SolrDocument.find(id)
      rescue Blacklight::Exceptions::RecordNotFound
        return false
      end
    end

    private

    def verify_files 
      file_errors = []
      get_spreadsheet.each_with_index do |row, row_num|
        file_fields = row.select { |field, value| is_file_field?(field) }
        file_fields.each do |column_name, filestring|
          next if filestring.blank? or column_name == filestring
          get_file_paths(filestring).each do |filepath|
            file_errors << BulkOps::Error.new({type: :cannot_find_file, file: filepath}) unless  File.file? filepath
          end
        end
      end
      @verification_errors.concat file_errors
      return file_errors
    end

    def verify_configuration
      BulkOps::Operation::OPTION_REQUIREMENTS.each do |option_name, option_info|
        # Make sure it's present if required
        if (option_info["required"].to_s == "true") || (option_info["required"].to_s == type)
          if options[option_name].blank?
            @verification_errors << BulkOps::Error.new({type: :missing_required_option, option_name: option_name})
          end
        end
        # Make sure the values are acceptable if present
        unless (values = option_info.values).blank? || options[option_name].blank?
          unless values.include? option[option_name]
            values_string = values.reduce{|a,b| "#{a}, #{b}"}
            @verification_errors << BulkOps::Error.new({type: :invalid_config_value, option_name: option_name, option_values: values_string})
          end        
        end
      end    
    end

    def downcase_first_letter(str)
      str[0].downcase + str[1..-1]
    end

    # Make sure the headers in the spreadsheet are matching to properties
    def verify_column_headers
      
      unless (headers = get_spreadsheet.headers)
        # log an error if we can't get the metadata headers
        @verification_errors << BulkOps::Error.new({type: :bad_header, field: column_name})      
      end

      headers.each do |column_name|
        next if column_name.blank?
        column_name_redux = column_name.downcase.parameterize.gsub(/[_\s-]/,"")
        # Ignore everything marked as a label
        next if column_name_redux.ends_with? "label"
        # Ignore any column names with special meaning in hyrax
        next if BulkOps::Operation::SPECIAL_COLUMNS.any?{|col| col.downcase.parameterize.gsub(/[_\s-]/,"") == column_name_redux }
        # Ignore any columns speficied to be ignored in the configuration
        ignored = options["ignored headers"] || []
        next if ignored.any?{|col| col.downcase.parameterize.gsub(/[_\s-]/,"") == column_name_redux }
        # Column names corresponding to work attributes are legit
        next if Work.attribute_names.any?{|col| col.downcase.parameterize.gsub(/[_\s-]/,"") == column_name_redux }
        @verification_errors << BulkOps::Error.new({type: :bad_header, field: column_name})
      end
    end

    def verify_remote_urls
      row_offset = BulkOps::GithubAccess::ROW_OFFSET.present? ? BulkOps::GithubAccess::ROW_OFFSET : 2
      get_spreadsheet.each_with_index do |row, row_num|
        update(message: "verifying controlled vocab urls (row number #{row_num})")
        next if row_num.nil?
        schema.controlled_field_names.each do |controlled_field_name|
          next unless (urls = row[controlled_field_name])
          urls.split(';').each do |url|
            label = ::WorkIndexer.fetch_remote_label(url)
            # if we can't get the label, and we aren't going to add a local label, throw an error
            if (!label || label.blank?) && !schema.get_field(controlled_field_name).vocabularies.any?{|vocab| vocab["authority"].to_s.downcase == "local"}
                @verification_errors << BulkOps::Error.new({type: :cannot_retrieve_label, row_number: row_num + row_offset, field: controlled_field_name, url: url})
            end
          end
        end
      end
    end

    def get_id_from_row row
      ref_id = get_ref_id(row).to_sym
      return :id if ref_id == :id
      normrow = row.mapgsub(//,'').parameterize
      if row.key?(ref_id)
        
        # TODO if ref_id is another column
        # TODO implement solr search 
      end
    end

    def verify_works_to_update
      return [] unless operation_type == "update"
      get_spreadsheet.each_with_index do |row, row_num|
        id = get_ref_id(row)
        #TODO: find by other field. for now just id
        unless (record_exists(id))
          @verification_errors << BulkOps::Error.new(type: :cannot_find_work, id: id)
        end
      end
    end

    def get_ref_id row
      row.each do |field,value| 
        next if field.blank? or value.blank? or field === value
        next unless BulkOps::WorkProxy::REFERENCE_IDENTIFIER_FIELDS.any?{ |ref_field| normalize_field(ref_field) ==  normalize_field(field) }
        return value 
      end
      # No reference identifier specified in the row. Use the default for the operation.
      return reference_identifier || :id
    end

    def normalize_field field
      return '' if field.nil?
      field.downcase.parameterize.gsub(/[_\s-]/,'')
    end

    def verify_internal_references
      # TODO 
      # This is sketchy. Redo it.
      get_spreadsheet.each do |row,row_num|
        ref_id = get_ref_id(row)
        BulkOps::Operation::RELATIONSHIP_COLUMNS.each do |relationship|
          next unless (obj_id = row[relationship])
          if (split = obj_id.split(':')).count == 2
            ref_id = split[0].downcase
            obj_id = split[1]
          end
          
          if ref_id == "row" || (ref_id == "id/row" && obj_id.is_a?(Integer))
            # This is a row number reference. It should be an integer in the range of possible row numbers.
            unless obj_id.is_a? Integer && obj_id > 0 && obj_id <= metadata.count
              @verification_errors << BulkOps::Error.new({type: :bad_object_reference, object_id: obj_id, row_number: row_num + ROW_OFFSET})
            end  
          elsif ref_id == "id" || ref_id == "hyrax id" || (ref_id == "id/row" && (obj_id.is_a? Integer))
            # This is a hydra id reference. It should correspond to an object already in the repo
            unless record_exists?(obj_id)
              @verification_errors << BulkOps::Error.new({type: :bad_object_reference, object_id: obj_id, row_number: row_num+ROW_OFFSET})
            end
          else

            # This must be based on some other presumably unique field in hyrax, or a dummy field in the spreadsheet. We haven't added this functionality yet. Ignore for now.

          end
        end      
      end
    end

  end
end
