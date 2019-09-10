# frozen_string_literal: true

require "kafka"
require "timeout"

module Mimi
  module Messaging
    module Kafka
      #
      # Kafka adapter class
      #
      # An adapter implementation must implement the following methods:
      # * #start()
      # * #stop()
      # * #command(target, message, opts)
      # * #query(target, message, opts)
      # * #event(target, message, opts)
      # * #start_request_processor(queue_name, processor, opts)
      # * #start_event_processor(topic_name, processor, opts)
      # * #start_event_processor_with_queue(topic_name, queue_name, processor, opts)
      # * #stop_all_processors
      #
      class Adapter < Mimi::Messaging::Adapters::Base
        attr_reader :options, :connection

        register_adapter_name "kafka"

        DEFAULT_OPTIONS = {
          mq_namespace: nil,
          mq_default_query_timeout: 15, # seconds,
          mq_kafka_namespace_separator: '--',
          mq_kafka_max_poll_interval: 0.010,
          mq_kafka_default_topic_num_partitions: 3,
          # :mq_default_topic_replication_factor is supposed to be set by the broker config
          mq_kafka_default_queue_topic_name_prefix: "queue.",
          mq_kafka_default_event_topic_name_prefix: nil,
          mq_kafka_default_consumer_group_name_prefix: nil
        }.freeze

        # Initializes Kafka adapter
        #
        # @param options [Hash]
        # @option options [String] :mq_adapter
        # @option options [String] :mq_host
        # @option options [String] :mq_port
        # @option options [String] :mq_username
        # @option options [String] :mq_password
        # @option options [String] :mq_namespace
        # @option options [Integer] :mq_default_query_timeout
        # @option options [Object] :mq_kafka_... Kafka specific options
        #
        def initialize(options)
          validate_options(options)
          @options = DEFAULT_OPTIONS.merge(options).dup
        end

        def start
          @connection_mutex = Mutex.new
          @connection = ::Kafka.new(kafka_hosts, logger: options[:mq_logger])
        end

        def stop
          @consumers&.each(&:stop)
          @consumers = nil
          @reply_listener&.stop
          @reply_listener = nil
          @connection = nil
          @connection_mutex = nil
        end

        def command(target, message, _opts = {})
          queue_name, method_name = target.split("/")
          message_payload = serialize(message)
          deliver_message(message_payload, queue_name, __method: method_name)
        end

        def query(target, message, opts = {})
          queue_name, method_name = target.split("/")
          message_payload = serialize(message)
          request_id = SecureRandom.hex(8)
          reply_queue = reply_listener.register_request_id(request_id)

          deliver_message(
            message_payload,
            queue_name,
            __method: method_name,
            __reply: reply_listener.kafka_topic,
            __request_id: request_id
          )
          timeout = opts[:timeout] || options[:mq_default_query_timeout]
          response = nil
          Timeout::timeout(timeout) do
            response = reply_queue.pop
          end
          deserialize(response.value)
        end

        def event(target, message, _opts = {})
          topic_name, event_type = target.split("/")
          message_payload = serialize(message)
          deliver_message(message_payload, topic_name)
        end

        def start_request_processor(queue_name, processor, opts = {})
          super
          @consumers ||= []
          opts = opts.dup
          opts[:num_partitions] ||= options[:mq_default_topic_num_partitions]
          opts[:replication_factor] ||= options[:mq_default_topic_replication_factor]
          create_topic(queue_name, opts.compact)
          @consumers << Consumer.new(self, queue_name, queue_name) do |m|
            message = deserialize(m.value)
            method_name = m.headers["__method"]
            reply_to = m.headers["__reply"]
            if reply_to
              response = processor.call_query(method_name, message, headers: m.headers)
              deliver_message(serialize(response), reply_to, __request_id: m.headers["__request_id"])
            else
              processor.call_command(method_name, message, headers: m.headers)
            end
          end
        end

        def start_event_processor(topic_name, processor, opts = {})
        end

        def start_event_processor_with_queue(topic_name, queue_name, processor, opts = {})
        end

        private

        # Returns a list of Kafka (seed) hosts
        #
        # @return [Array<String>] e.g. ["kafka-1:9092", "kafka-2:9092"]
        #
        def kafka_hosts
          host = "#{options[:mq_host]}:#{options[:mq_port]}"
          [host]
        end

        def validate_options(options)
          required_options = %i[mq_host mq_port mq_username mq_password]
          required_options.each do |name|
            unless options.key?(name)
              raise Mimi::Messaging::ConfigurationError, ":#{name} is expected to be set"
            end
          end
        end

        def logger
          options[:mq_logger]
        end

        # Delivers a message to a given topic.
        #
        # The topic will NOT be created if it does not exist.
        #
        # NOTE: the kafka broker may still automatically create a topic
        #
        def deliver_message(message, kafka_topic, headers = {})
          @connection_mutex.synchronize do
            connection.deliver_message(message, topic: kafka_topic, headers: headers)
          end
        rescue StandardError => e
          logger&.warn "Failed to deliver message to '#{kafka_topic}': #{e}"
        end

        # Create topic if it does not exist
        #
        # @param kafka_topic [String] name of the topic to be created
        # @param opts [Hash] options to a new topic
        #
        def create_topic(kafka_topic, opts = {})
          puts "Creating topic #{kafka_topic} with opts: #{opts}"
          connection.create_topic(kafka_topic, opts)
        rescue ::Kafka::TopicAlreadyExists
          puts "Topic #{kafka_topic} already exists"
          nil
        end

        # Returns the configured reply listener for this process
        #
        # @return [ReplyListener]
        #
        def reply_listener
          @reply_listener ||= begin
            reply_topic_name = "reply.#{SecureRandom.hex(8)}.#{Time.now.to_i}"
            Mimi::Messaging::Kafka::ReplyListener.new(self, reply_topic_name)
          end
        end
      end # class Adapter
    end # module Kafka
  end # module Messaging
end # module Mimi
