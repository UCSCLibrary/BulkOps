# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
#app = File.expand_path("../app", __FILE__)
#$LOAD_PATH.unshift(app) unless $LOAD_PATH.include?(app)
require "bulk_ops/version"

Gem::Specification.new do |spec|
  spec.name          = "bulk_ops"
  spec.version       = BulkOps::VERSION
  spec.authors       = ["Ned Henry, UCSC Library Digital Initiatives"]
  spec.email         = ["ethenry@ucsc.edu"]

  spec.summary       = %q{A gem to add bulk ingest and bulk update functionality to Hyrax (Samvera) applications.}
  spec.description   = %q{A gem to add bulk ingest and bulk update functionality to Hyrax (Samvera) applications.}
  spec.homepage      = "http://UCSCLibrary.github.org"
  spec.license       = "MIT"

  spec.files = Dir["{app,config,db,lib}/**/*"]
  spec.test_files = Dir["spec/**/*"]
#
#
#  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
#    f.match(%r{^(test|spec|features)/})
#  end
#  spec.bindir        = "exe"
#  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
#  spec.require_paths = ["lib", "app"]
#
#  spec.add_runtime_dependency "hyrax", "~> 2"
  spec.add_dependency "rails", "~> 5"
#  spec.add_dependency "hydra-access-controls", "~> 10"
 # spec.add_dependency "hyrax", "~> 2"

  spec.add_development_dependency "bundler", "~> 1.15"
  spec.add_development_dependency "rake", "~> 10.0"
end
