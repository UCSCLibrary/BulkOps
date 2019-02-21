require "bulk_ops/version"

module BulkOps
  dirstring = File.join( File.dirname(__FILE__), 'bulk_ops/**/*.rb')
  Dir[dirstring].each  do |file| 
    begin
      require file 
    rescue Exception => e
      puts "ERROR LOADING #{File.basename(file)}: #{e}"
    end
  end
#  require 'bulk_ops/verification'
#  require 'bulk_ops/verification'
#  require 'bulk_ops/work_proxy'
end
