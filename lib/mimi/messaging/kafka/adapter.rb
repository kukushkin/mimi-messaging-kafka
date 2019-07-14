# frozen_string_literal: true

module Mimi
  module Messaging
    module Kafka
      #
      # Kafka adapter class
      #
      class Adapter < Mimi::Messaging::Adapters::Base
        register_adapter_name "kafka"

        # Initializes Kafka adapter
        #
        # @param options [Hash]
        # @option options [String] :mq_adapter
        # @option options [String] :mq_host
        # @option options [String] :mq_port
        # @option options [String] :mq_username
        # @option options [String] :mq_password
        #
        def initialize(options)
          validate_options(options)
          @options = options.dup
        end

        private

        def validate_options(options)
          required_options = %i[mq_host mq_port mq_username mq_password]
          required_options.each do |name|
            unless options.key?(name)
              raise Mimi::Messaging::ConfigurationError, ":#{name} is expected to be set"
            end
          end
        end
      end # class Adapter
    end # module Kafka
  end # module Messaging
end # module Mimi
