require 'rails/generators'
class BulkOps::InstallGenerator < Rails::Generators::Base
  source_root File.expand_path('../templates', __FILE__)

  def inject_routes
    insert_into_file "config/routes.rb", :after => ".draw do" do
      %{\n  mount BulkOps::Engine => '/'\n}
    end
  end

  def inject_compile_assets
    insert_into_file "config/initializers/assets.rb", :before => /^end/ do
      %{\nRails.application.config.assets.precompile += %w( bulk_ops.js )\nRails.application.config.assets.precompile += %w( bulk_ops.css )\n}
    end
  end

  def inject_sidebar_widget
    append_to_file "app/views/hyrax/dashboard/sidebar/_ingests.html.erb" do
      %{\n <%= render 'bulk_ops/bulk_ops_sidebar_widget', menu: menu %> \n}
    end
  end

  def copy_github_config
    copy_file "config/github.yml.example", "config/github.yml"
  end

end
