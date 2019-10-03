class BulkOps::CreateSpreadsheetJob < ActiveJob::Base
  
  queue_as :default

  def perform(branch_name, work_ids, fields, user)
    csv_file = Tempfile.new('bulk_ops_metadata')
    csv_file.write(fields.join(','))
    work_ids.each do |work_id| 
      if work_csv = work_to_csv(work_id,fields)
        csv_file.write("\r\n" + work_csv)
      end
    end
    csv_file.close
    BulkOps::GithubAccess.new(branch_name, user).add_new_spreadsheet(csv_file.path)
    csv_file.unlink
  end

  private

  def work_to_csv work_id, fields
    return false if work_id.empty?
    begin
      work = Work.find(work_id)
    rescue ActiveFedora::ObjectNotFoundError
      return false
    end
    line = ''
    fields.map do |field_name| 
      label = false
      if field_name.downcase.include? "label"
        label = true
        field_name = field_name[0..-7]
      end
      values = work.send(field_name)
      values.map do |value|
        next if value.is_a? DateTime 
        value = (label ? WorkIndexer.fetch_remote_label(value.id) : value.id) unless value.is_a? String
        value.gsub("\"","\"\"")
      end.join(BulkOps::SEPARATOR).prepend('"').concat('"')
    end.join(',')
  end

end
