class RemoveRelationshipsAmmendWorkProxy < ActiveRecord::Migration[5.0]
  def change

    drop_table :bulk_ops_relationships

    change_table :bulk_ops_work_proxies do |t|
      t.integer :parent_id
      t.integer :previous_sibling_id
    end

  end
end
