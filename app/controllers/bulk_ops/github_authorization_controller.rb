class BulkOps::GithubAuthorizationController < ApplicationController

  def authorize
    return false unless BulkOps::GithubAccess.valid_state?(params['state'],params['user_id'])
    BulkOps::GithubAccess.set_auth_token! params['code'], params['user_id']
    redirect_target = session[:git_auth_redirect] || "/bulk_ops/operations"
    redirect_to redirect_target, notice: "Successfully logged in to Github"
  end

  # DELETE /github_credentials/1
  # DELETE /github_credentials/1.json
  def logout
    BulkOps::GithubCredential.find_by(user_id: current_user.id).destroy
    respond_to do |format|
      redirect_target = session[:git_auth_redirect] || "/bulk_ops/operations"
      format.html { redirect_to redirect_target, notice: 'Logged out of github' }
      format.json { head :no_content }
    end
  end

  private
  # Use callbacks to share common setup or constraints between actions.
  def set_github_credential
    @github_credential = GithubCredential.find(params[:id])
  end

  # Never trust parameters from the scary internet, only allow the white list through.
  def github_credential_params
    params.require(:github_credential).permit(:user_id, :username, :oauth_code)
  end


end
