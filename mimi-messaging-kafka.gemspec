# frozen_string_literal: true

lib = File.expand_path("lib", __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "mimi/messaging/kafka/version"

Gem::Specification.new do |spec|
  spec.name        = "mimi-messaging-kafka"
  spec.version     = Mimi::Messaging::Kafka::VERSION
  spec.authors     = ["Alex Kukushkin"]
  spec.email       = ["alex@kukushk.in"]

  spec.summary     = "Kafka adapter for mimi-messaging"
  spec.description = "Kafka adapter for mimi-messaging"
  spec.homepage    = "https://github.com/kukushkin/mimi-messaging-kafka"
  spec.license     = "MIT"

  spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/kukushkin/mimi-messaging-kafka"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "mimi-messaging" # , "~> 1.0"
  spec.add_dependency "ruby-kafka"

  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "pry", "~> 0.12"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
end
