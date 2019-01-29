module BulkOps
  module Verification
    extend ActiveSupport::Concern
    
    def verify!
      @verification_errors ||= []
      verify_column_headers
      verify_remote_urls
      verify_internal_references
      verify_files
      unless @verification_errors.blank?
        error_file_name = BulkOps::Error.write_errors!(@verification_errors, git)
        #notify everybody
        notify!(subject: "Errors verifying bulk #{operation_type} in Hycruz", message: "Hyrax ran a verification step to make sure that the spreadsheet for this bulk #{operation_type} is formatted correctly and won't create any errors. We found some problems. You can see a summary of the issues at this url: https://github.com/#{git.repo}/blob/#{git.name}/#{git.name}/errors/#{error_file_name}. Please fix these problems and run this verification again. The bulk #{operation_type} will not be allowed to move forward until all verification issues are resolved.")

        return false
      end
      return true
    end

    private

    def verify_files 
      get_spreadsheet.each_with_index do |row, row_num|
        file_string = ""
        next unless (file_string = row["file"]) || (file_string = row["filename"])
        next if row_num < 1 && ["filename","file"].include?(file_string)
        filenames = file_string.split(';')
        filenames.each do |filename|
          next if File.file? File.join(BulkOps::Operation::BASE_PATH,filename)
          @verification_errors << BulkOps::Error.new({type: :cannot_find_file, file: filename})
        end
      end
      return errors
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
      return errors
    end

    def downcase_first_letter(str)
      str[0].downcase + str[1..-1]
    end

    # Make sure the headers in the spreadsheet are matching to properties
    def verify_column_headers
      
      errors = {cannot_get_headers:[],
                bad_header:[]}
      unless (headers = get_spreadsheet.headers)
        # log an error if we can't get the metadata headers
        errors[:cannot_get_headers] << true
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
      return errors
    end

    def verify_remote_urls
      get_spreadsheet.each do |row, row_num|
        schema["controlled"].each do |controlled_field|
          next unless (url = row[controlled_field])
          label = ::WorkIndexer.fetch_remote_label(url)        
          if !label || label.blank?
            @verification_errors << BulkOps::Error.new({type: :cannot_retrieve_label, row: row_num + ROW_OFFSET, field: controlled_field, url: url})
          end
        end
      end
      return errors
    end

    def verify_internal_references
      get_spreadsheet.each do |row,row_num|
        ref_id = row['reference_identifier'] || reference_identifier
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
            unless SolrDocument.find(obj_id) || ActiveFedora::Base.find(obj_id)
              @verification_errors << BulkOps::Error.new({type: :bad_object_reference, object_id: obj_id, row_number: row_num+ROW_OFFSET})
            end
          else

            # This must be based on some other presumably unique field in hyrax, or a dummy field in the spreadsheet. We haven't added this functionality yet. Ignore for now.

          end
        end      
      end
    end

    def notify!(subject: , message:)
      options["notifications"].each do |email|
        ActionMailer::Base.mail(from: "admin@digitalcollections.library.ucsc.edu",
                                to: email,
                                subject: subject,
                                body: message).deliver
      end
    end

  end
end
