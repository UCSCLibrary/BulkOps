module BulkOps
  class ApplicationController < ActionController::Base
    protect_from_forgery with: :exception
    include Blacklight::Controller
    include Hydra::Controller::ControllerBehavior

    # Adds Hyrax behaviors into the application controller 
    include Hyrax::Controller

    include Hyrax::ThemedLayoutController
    with_themed_layout '1_column'
  end
end
