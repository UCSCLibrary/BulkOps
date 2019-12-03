class BulkOps::Error
  attr_accessor :type, :row_number, :object_id, :message, :option_name, :file, :option_values, :field, :url

  MAX_ERROR = 5000
  MAX_ERROR_SHORT = 50

  def initialize type:, row_number: nil, object_id: nil, message: nil, options_name: nil, option_values: nil, field: nil, url: nil , file: nil
    @type = type
    @row_number = row_number
    @object_id = object_id
    @message = message
    @option_name = option_name
    @option_values = option_values
    @field = field
    @file = file
    @url = url
  end

  def self.write_errors! errors, git
    return false if errors.blank?
    error_file_name = "error_log_#{DateTime.now.strftime("%F-%H%M%p")}.log"

    error_hash = {}

    errors.sort!{|x,y| x.type <=> y.type}
    error_types = errors.map{|error| error.type}.uniq

    #write errors to error file
    error_file = Tempfile.new(error_file_name)
    error_types.each do |error_type|
      typed_errors = errors.select{|er| er.type == error_type}
      next if typed_errors.blank?
      message = self.error_message(error_type, typed_errors)
      puts "Error message: #{message}"
      error_file.write(message)
    end
    error_file.close
    git.add_file error_file.path, File.join("errors", error_file_name)
    error_file.unlink
    return error_file_name
  end

  def self.error_message type, errors, short=false
    max_error = short ? MAX_ERROR_SHORT : MAX_ERROR
    case type
    when :mismatched_auth_terms
      message = "\n-- Controlled Authority IDs and Labels don't match -- \n"
      message += "The operation is set to create an error if the provided URLs for controlled authority terms do not resolve to the provided labels.\n"
      if errors.count < max_error
        message += "The following rows were affected:\n"
        message += errors.map{|error| error.row_number}.join(",")+"\n"
      else
        message += "#{errors.count} rows were affected. An example is row # #{errors.first.row_number}.\n"
      end
    when :upload_error
      message = "\n-- Errors uploading files -- \n"
      message += "Your files looked ok when we checked earlier, but we couldn't access them when we were trying to actually start the operation.\n"
      if errors.count < max_error
        message += "The following files were affected:\n"
        message += errors.map{|error| "Row #{row_number}, Filename: #{error.file}"}.join("\n")+"\n" 
      else
        message += "#{errors.count} rows were affected. An example is row # #{errors.first.row_number} with file #{errors.first.file}.\n"
      end

    when :no_work_id_field
      message = "\n-- Cannot find work id field in spreadsheet -- \n"
      message += "We were trying to start your operation, but could find find the work id for #{errors.count} different rows of the spreadsheet.\n"
      message += "Check your spreadsheet and try again.\n"
      message += errors.map{|arg| "  #{error.object_id || 'new work'}: #{error.message}"}.join("\n") + "\n"
    when :job_failure
      message = "\n-- Jobs Failed -- \n:"
      message += errors.map{|arg| "Error message operating on #{error.object_id || 'new work'}: #{error.message}"}.join("\n") + "\n"
    when :missing_required_option 
      message = "\n-- Errors in configuration file -- \nMissing required option(s):"
      message += errors.map{|arg| error.option_name}.join(", ") + "\n"

    when :invalid_config_value 
      message = "\n-- Errors in configuration file values --\n" 
      errors.each do |error|
        message += "Unacceptable value for #{error.option_name}. Acceptable values include: #{error.option_values}\n"
      end

    when :cannot_get_headers 
      message += "\n-- Error Retrieving Field Headers --\n"
      message += "We cannot retrieve the column headers from metadata spreadsheet on github,\nwhich define the fields for the metadata below.\nEither the connection to github is failing, \nor the metadata spreadsheet on this branch is not properly formatted.\n"

    when :bad_header 
      message =  "\n-- Error interpreting column header(s) --\n"
      message += "We cannot interpret all of the headers from your metadata spreadsheet. \nSpecifically, the following headers did not make sense to us:\n"
      message += errors.map{|error| error.field}.join(", ")+"\n"

    when :cannot_retrieve_label 
      message = "\n-- Errors Retrieving Remote Labels --\n"
      urls = errors.map{|error| error.url}.uniq
      if urls.count < max_error
        urls.each do |url|
          url_errors = errors.select{|er| er.url == url}
          message +=  "Error retrieving label for remote url #{url}. \nThis url appears in #{url_errors.count} instances in the spreadsheet.\n"
          message += "The affected rows are listed here:\n"
          message += url_errors.map{|er| er.row_number}.compact.join('\n')+"\n"
        end
      else
        message += "There were #{urls.count} different URLs in the spreadsheet that we couldn't retrieve labels for,\n making a total of #{errors.count} url related errors.\n These are too many to list, but an example is #{errors.first.url}\n in row #{errors.first.row_number}.\n"
      end

    when :cannot_retrieve_url
      message = "\n-- Errors Retrieving Remote URLs --\n"
      urls = errors.map{|error| error.url}.uniq
      if urls.count < max_error
        urls.each do |url|
          url_errors = errors.select{|er| er.url == url}
          message +=  "Error retrieving URL for remote authority term #{url}. \nThis term appears in #{url_errors.count} instances in the spreadsheet.\n"
          message += "The affected rows are listed here:\n"
          message += url_errors.map{|er| er.row_number}.compact.join('\n')+"\n"
        end
      else
        message += "There were #{urls.count} different controlled vocab terms in the spreadsheet that we couldn't retrieve or create URLs for,\n making a total of #{errors.count} controlled term related errors.\n These are too many to list, but an example is #{errors.first.url}\n in row #{errors.first.row_number}.\n"
      end

    when :bad_object_reference 
      message = "\n-- Error: bad object reference --\n" 
      message += "We enountered #{errors.count} problems resolving object references.\n"
      if errors.count < max_error
         message += "The row numbers with problems were:\n"
         message += errors.map{|er| "row number #{er.row_number} references the object #{er.object_id}"}.join("\n")
      else
         message += "For example, row number #{errors.first.row_number} references an object identified by #{errors.first.object_id}, which we cannot find."
      end
              
    when :cannot_find_file
      message = "\n-- Missing File Errors --\n "
      message += "We couldn't find the files listed on #{errors.count} rows.\n"
      if errors.count < max_error
        message += "Missing filenames:\n"
        message += errors.map{|er| er.file}.join("\n")
      else
        message += "An example of a missing filename is: #{errors.first.file}\n"
      end
      
   when :relationship_error
      message = "\n-- Errors resolving relationships --\n "
      message += "There were issues resolving #{errors.count} relationships.\n"
      if errors.count < max_error
        message += "errors:\n"
        message += errors.map{|er| "Row #{er.row_number}, relationship ##{er.object_id}: #{er.message}"}.join("\n")
      else
        message += "An example of an error is: Row #{er.first.row_number}, relationship ##{er.first.object_id}: #{er.first.message}\n"
      end
      
    when :ingest_failure
      message = "\n-- Ingested File is Broken or Missing --\n "
      message += "After the ingest completed, we had issues finding and re-saving the ingested works associated with #{errors.count} rows.\n"
      if errors.count < max_error
        message += "Problem rows:\n"
        message += errors.map{|er| "#{er.row_number} - proxy ##{er.object_id}"}.join("\n")
      else
        message += "An example of a failed ingest is row #{errors.first.row_number} with work proxy #{errors.first.object_id} \n"
      end

    when :id_not_unique
      message = "\n-- Multiple works shared a supposedly unique identifier, and we don't know which one to edit --\n "
      if errors.count < max_error
        message += "Problem rows:\n"
        message += errors.map{|er| "#{er.row_number} - proxy ##{er.object_id} - #{er.options_name}: #{er.option_values}"}.join("\n")
      else
        message += "An example of a row that identifies multiple works is #{errors.first.row_number} with work proxy #{errors.first.object_id} using the identifier:  #{er.options_name} - #{er.option_values} \n"
      end

    else
      message = "\n-- There were other errors of an unrecognized type. Check the application logs --\n "      
    end
    return message
  end
end
