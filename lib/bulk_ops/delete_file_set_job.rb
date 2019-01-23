#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

class BulkOps::DeleteFileSetJob < ActiveJob::Base

  queue_as :ingest

  def perform(file_set_id,user_email)
    user = User.find_by_email(user_email)
    Hyrax::Actors::FileSetActor.new(@file_set, user).destroy
  end

  private

end
