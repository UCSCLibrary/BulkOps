class CreateGithubCredentials < ActiveRecord::Migration[5.0]
  def change
    create_table :bulk_ops_github_credentials do |t|
      t.integer :user_id
      t.string :username
      t.string :oauth_token
      t.string :state

      t.timestamps
    end
    add_index :bulk_ops_github_credentials, :user_id
  end
end
