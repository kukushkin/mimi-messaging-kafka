# frozen_string_literal: true

module Mimi
  module Messaging
    module Kafka
      #
      # ReplyListener listens on a particular Kafka topic for replies
      # and passes them to registered Queues (see Ruby ::Queue class).
      #
      class ReplyListener
        attr_reader :kafka_topic

        def initialize(adapter, kafka_topic)
          @mutex = Mutex.new
          @queues = {}
          @kafka_topic = kafka_topic
          @adapter = adapter
          create_topic(kafka_topic)
          @consumer = Consumer.new(adapter, kafka_topic, "#{kafka_topic}.listener") do |message|
            dispatch_message(message)
          end
          puts "ReplyListener is listening on #{kafka_topic}"
        end

        def stop
          begin
            @consumer.stop
          rescue StandardError => e
            puts "FAILED to stop consumer: #{e}"
          end
          @adapter.connection.delete_topic(kafka_topic)
        end

        # Register a new request_id to listen for.
        #
        # Whenever the message with given request_id will be received,
        # it will be dispatched to a returned Queue.
        #
        # @param request_id [String]
        # @return [Queue] a new Queue object registered for this request_id
        #
        def register_request_id(request_id)
          queue = Queue.new
          @mutex.synchronize do
            queue = @queues[request_id] ||= queue
          end
          queue
        end

        private

        # Dispatch message received on a reply topic
        #
        # @param message [Kafka::FetchedMessage]
        #
        def dispatch_message(message)
          queue = nil
          @mutex.synchronize do
            request_id = message.headers["__request_id"]
            queue = @queues.delete(request_id)
          end
          queue&.push(message)
        end

        # Create reply topic if it is not created yet
        #
        # @param kafka_topic [String] name of the reply topic
        #
        def create_topic(kafka_topic)
          @adapter.connection.create_topic(kafka_topic)
        rescue ::Kafka::TopicAlreadyExists
          raise "Failed to create #{self.class}: " \
            "attempted to concurrently listen on topic: #{kafka_topic}"
        end
      end # class ReplyListener
    end # module Kafka
  end # module Messaging
end # module Mimi
