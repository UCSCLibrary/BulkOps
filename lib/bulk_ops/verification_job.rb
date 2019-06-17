#require 'hydra/access_controls'
#require 'hyrax/workflow/activate_object'

class BulkOps::VerificationJob < ActiveJob::Base

  attr_accessor :operation

  queue_as :default

  def perform(operation_id)
    operation = BulkOps::Operation.find(operation_id)
    if operation.verify
      operation.set_stage "authorize"
      if operation.create_pull_request
        operation.notify(subject: "Bulk Operation Verification Successful", message: "Your bulk ingest has passed verification, and we have requested to start applying the operation. It may required one final approval from an administrator before the operation proceeds.")
      else
        operation.notify(subject: "Bulk Operation - Error creating Github pull request", message: "Your bulk ingest has passed verification, but we had a problem creating a pull request on Github in order to merge this operation with the master branch. Please check your github configuration.")
      end
    else
      operation.set_stage "pending"
    end
  end
end
