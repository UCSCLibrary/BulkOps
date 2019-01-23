module BulkOps::SearchBuilderBehavior
  extend ActiveSupport::Concern
  included do
    attr_reader :collection, 
                :admin_set, 
                :workflow_state
    class_attribute :collection_field, 
                    :collection_id_field, 
                    :admin_set_field, 
                    :admin_set_id_field, 
                    :workflow_state_field, 
                    :workflow_state_id_field, 
                    :keyword_field
    self.collection_field = 'member_of_collections_ssim'
    self.collection_id_field = 'member_of_collection_ids_ssim'
    self.admin_set_field = 'admin_set_tesim'
    self.admin_set_id_field = 'isPartOf_ssim'
    self.workflow_state_field = 'workflow_state_name_ssim'
    self.keyword_field = 'all_fields'
    
    self.default_processor_chain += [:member_of_collection, 
                                   :member_of_admin_set, 
                                   :in_workflow_state, 
                                   :with_keyword_query]
  end
  # @param [scope] Typically the controller object
  def initialize(scope: {}, 
                 collection: nil, 
                 collection_id: nil, 
                 admin_set: nil, 
                 admin_set_id: nil, 
                 workflow_state: nil, 
                 keyword_query: nil) 

    @collection = collection unless collection.blank?
    @admin_set = admin_set unless admin_set.blank?
    @admin_set_id = admin_set_id unless admin_set_id.blank?
    @workflow_state = workflow_state unless workflow_state.blank?
    @collection_id = collection_id unless collection_id.blank?
    @workflow_state = workflow_state unless workflow_state.blank?
    @keyword_query = keyword_query unless keyword_query.blank?
    super(scope)
  end

  def models
    [Work,Course,Lecture]
  end
  
  # include filters into the query to only include the collection memebers
  def member_of_collection(solr_parameters)
    solr_parameters[:fq] ||= []
    solr_parameters[:fq] << "#{collection_field}:#{@collection}" if @collection
    solr_parameters[:fq] << "#{collection_id_field}:#{@collection_id}" if @collection_id
  end

  # include filters into the query to only include the collection memebers
  def member_of_admin_set(solr_parameters)
    solr_parameters[:fq] ||= []
    solr_parameters[:fq] << "#{admin_set_field}:#{@admin_set}" if @admin_set
    solr_parameters[:fq] << "#{admin_set_id_field}:#{@admin_set_id}" if @admin_set_id
  end

  # include filters into the query to only include the collection memebers
  def in_workflow_state(solr_parameters)
    solr_parameters[:fq] ||= []
    solr_parameters[:fq] << "#{workflow_state_field}:#{@workflow_state}" if @workflow_state
  end

  def with_keyword_query(solr_parameters)
    if @keyword_query
      solr_parameters[:q] ||= []
      #    solr_parameters[:q] << "#{keyword_field}:#{@keyword_query}" if @keyword_query
      solr_parameters[:q] << @keyword_query 
      solr_parameters[:qf] = "title_tesim titleAlternative_tesim subseries_tesim creator_label_tesim contributor_label_tesim originalPublisher_tesim publisher_tesim publisherHomepage_tesim resourceType_label_tesim  rightsHolder_label_tesim scale_tesim series_tesim source_tesim staffNote_tesim coordinates_tesim subjectName_label_tesim subjectPlace_label_tesim subjectTemporal_label_tesim subjectTopic_label_tesim dateCreated_tesim dateCreatedDisplay_tesim dateDigitized_tesim datePublished_tesim description_tesim physicalFormat_label_tesim keyword_tesim language_label_tesim license_tesim masterFilename_tesim physicalDescription_tesim accessRights_tesim itemCallNumber_tesim collectionCallNumber_tesim donorProvenance_tesim genre_label_tesim boxFolder_tesim subject_label_tesim file_format_tesim all_text_timv"
    end
    solr_parameters
  end

end
