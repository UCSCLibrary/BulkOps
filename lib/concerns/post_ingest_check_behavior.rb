module BulkOps::PostIngestCheckBehavior
  extend ActiveSupport::Concern

  def check
    @metadata.each_with_index do |row, row_number|
      unless BulkOps::Parser.is_file_set? @metadata, row_number
        # check that proxy exists
        proxy = @operation.work_proxies.where(row_number: row_number).first
        throw_error(:proxy_not_found, row_number) unless proxy.present?

        parser = BulkOps::Parser.new(proxy, @metadata, @operation.options)
        
        # check that work exists
        throw_error(:work_not_created, row_number) unless proxy.work_id.present?
        begin
          work = ActiveFedora::Base.find(proxy.work_id)
        rescue ActiveFedora::ObjectNotFoundError, Ldp::Gone
          throw_error(:work_not_found, row_number)
        end

        begin
          doc = SolrDocument.find(proxy.work_id)
        rescue Blacklight::Exceptions::RecordNotFound
          throw_error(:work_not_indexed, row_number)
        end

        total_files = 0
        
        #check metadata
        row.each do |field, values|
          next if field.blank? or values.blank?
          field = field.to_s
          normfield = field.downcase.parameterize.gsub(/[_\s-]/,'')
          
          # check if all files have been ingested
          if BulkOps::Verification.is_file_field?(field)
            ingested_filenames = work.file_sets.reduce([]){|filename_array, fs| filename_array + fs.original_file.file_name.to_a}
            BulkOps::Parser.split_values(values).each do |value|
              next if value.blank?
              total_files += 1
              throw_error(:missing_fileset, row_number, field, values) unless ingested_filenames.include?(value.split('/').last)
            end
          end

          # check if the work is the right type
          if ["objecttype","model","type","worktype"].include?(normfield)
            unless work.class.to_s.downcase.gsub(/[-_\s]/,'') == values.to_s.downcase.gsub(/[-_\s]/,'')
              throw_error(:wrong_type, row_number, field, values)
            end
          end

          # check metadata
          if (field_name = @operation.find_field_name(field))
            scooby_field = ScoobySnacks::METADATA_SCHEMA.get_field(field_name)
            BulkOps::Parser.split_values(values).each do |value|
              next if value.blank?
              if scooby_field.controlled?
                indexed_vals = doc.send(scooby_field.name)
                throw_error(:controlled_metadata_problem, row_number, field, values) unless (indexed_vals.include?(value) || indexed_vals.include?(WorkIndexer.fetch_remote_label(value)))
              else
                throw_error(:scalar_metadata_fedora_problem, row_number, field, values) unless work.send(scooby_field.name).include?(value)
                throw_error(:scalar_metadata_solr_problem, row_number, field, values) unless doc.send(scooby_field.name).include?(value)
              end
            end
          end

          # check parent relationship
          # For now, trust that parser has correctly assigned parent replationships to proxies
          if "parent" == BulkOps::Parser.normalize_relationship_field_name(field)
            #if the proxy has no parent proxy defined, probably the parent is a collection
            next unless proxy.parent_id
            if (parent_proxy = BulkOps::WorkProxy.find(proxy.parent_id))
              if parent_proxy.work_type == "Work"
                throw_error(:cannot_find_parent, row_number, field, values) unless (doc.parent_id == parent_proxy.work_id)
              elsif parent_proxy.work_type == "Collection"
                throw_error(:cannot_find_parent, row_number, field, values) unless (doc.member_of_collection_ids.include?(parent_proxy.work_id))
              end
            else
              begin
                parent_proxy_work = ActiveFedora::Base.find(proxy.parent_id)
                throw_error(:cannot_find_parent, row_number, field, values) unless (doc.member_of_collection_ids + [doc.parent_id]).include?(parent_proxy_work.id)
              rescue
                throw_error(:cannot_find_parent, row_number, field, values)
              end
            end
          end
         end
        # check total number of files (checks for doubles)
        throw_error(:wrong_file_number, row_number, "filename") unless (work.class != "Work" || work.file_sets.count == total_files)
        
      end
    end
  end

  def throw_error type, row, field=nil, value=nil
    @errors ||= {}
    #TODO use error class
    puts "Error: #{type.to_s.titleize} --- Row ##{row} #{field}"
    (@errors[type] ||= []) <<  "Row #{row} - #{field} - #{value}"
  end

end
