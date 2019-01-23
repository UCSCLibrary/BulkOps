class CreateBulkOpsTables < ActiveRecord::Migration[5.0]
  def change

    create_table :bulk_ops_operations do |t|
      t.references :user, foreign_key: true
      t.string :name, null: false, unique: true
      t.string :stage, null: false
      t.string :operation_type
      t.string :commit_sha
      t.integer :pull_id
      t.string :status
      t.text :message
      t.timestamps
    end

    create_table :bulk_ops_work_proxies do |t|
      t.integer :operation_id
      t.string :work_id
      t.integer :row_number
      t.datetime :last_event
      t.string :status
      t.text :message
      t.string :visibility
      t.string :work_type
      t.string :reference_identifier
      t.string :order
      t.timestamps
    end

    create_table :bulk_ops_relationships do |t|
      t.integer :work_proxy_id
      t.string   :object_identifier
      t.string   :identifier_type
      t.string   :relationship_type
      t.string   :status
      t.timestamps
    end

  end
end
