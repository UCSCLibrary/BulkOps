BulkOps::Engine.routes.draw do

  get 'bulk_ops/authorize/:user_id', to: 'github_authorization#authorize'
  post 'bulk_ops/github_logout/:user_id', to: 'github_authorization#logout'
  post 'bulk_ops/apply', to: 'operations#apply'

#  scope as: :bulk_ops, module:  :bulk_ops do
    resources :operations, path: :bulk_ops do

      collection do
        post :apply
        get :apply
        get :search
        post :search
      end

      member do
        post :request_apply
        post :approve
        post :edit
        get :csv
        get :info
        get :errors
        get :log
        post :duplicate
      end
    end
#  end

end
