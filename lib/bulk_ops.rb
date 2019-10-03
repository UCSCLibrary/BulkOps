require "bulk_ops/version"

module BulkOps
  OPTION_FIELDS = ['visibility','work type']
  RELATIONSHIP_FIELDS = ['parent','child','collection','order']
  REFERENCE_IDENTIFIER_FIELDS = ['Reference Identifier','ref_id','Reference ID','Relationship ID','Relationship Identifier','Reference Identifier Type','Reference ID Type','Ref ID Type','relationship_identifier_type','relationship_id_type']
  FILE_FIELDS = ['file','files','filename','filenames']
  FILE_ACTIONS = ['add','upload','remove','delete']
  SEPARATOR = ';'
  DEFAULT_ADMIN_SET_TITLE = "Bulk Ingest Set"
  INGEST_MEDIA_PATH = "/dams_ingest"
  TEMPLATE_DIR = "lib/bulk_ops/templates"
  RELATIONSHIP_COLUMNS = ["parent","child","next"]
  SPECIAL_COLUMNS = ["parent",
                     "child",
                     "order",
                     "next",
                     "work_type",
                     "collection", 
                     "collection_title",
                     "collection_id",
                     "visibility",
                     "relationship_identifier_type",
                     "id",
                     "filename",
                     "file"]
  IGNORED_COLUMNS = ["ignore","offline_notes"]
  OPTION_REQUIREMENTS = {type: {required: true, 
                                values:[:ingest,:update]},
                         file_method: {required: :true,
                                       values: [:replace_some,:add_remove,:replace_all]},
                         notifications: {required: true}}
  SPREADSHEET_FILENAME = 'metadata.csv'
  OPTIONS_FILENAME = 'configuration.yml'
  ROW_OFFSET = 2

  dirstring = File.join( File.dirname(__FILE__), 'bulk_ops/**/*.rb')
  Dir[dirstring].each  do |file| 
    begin
      require file 
    rescue Exception => e
      puts "ERROR LOADING #{File.basename(file)}: #{e}"
    end
  end

end
