BulkOps::Engine.routes.draw do

  get 'bulk_ops/authorize/:user_id', to: 'github_authorization#authorize'
  post 'bulk_ops/github_logout/:user_id', to: 'github_authorization#logout'
  post 'bulk_ops/apply', to: 'operations#apply'

  resources :operations, path: :bulk_ops do
    
    collection do
      get :search
      post :apply
      post :search
      post :destroy_multiple
    end

    member do
      get :csv
      post :request_apply
      post :approve
      post :edit
      post :duplicate
    end

  end
end
