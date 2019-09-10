# frozen_string_literal: true

module Mimi
  module Messaging
    module Kafka
      #
      # Message processor
      #
      class Consumer
        DEFAULT_MAX_POLL_INTERVAL = 0.010 # s, time between polls to fetch batches
        def initialize(adapter, topic, group_id, &block)
          @consumer = adapter.connection.consumer(group_id: group_id)
          @consumer.subscribe(topic)
          @block = block
          @consumer_thread = Thread.new do
            max_poll_interval = adapter.options[:mq_max_poll_interval] || DEFAULT_MAX_POLL_INTERVAL
            @consumer.each_message(
              max_poll_interval: max_poll_interval, automatically_mark_as_processed: false
            ) do |message|
              block.call(message)
              @consumer.mark_message_as_processed(message)
              @consumer.commit_offsets
              puts "Consumer committed offsets"
            end
            puts "Consumer for #{topic} stopped"
          end
        end

        def stop
          @consumer.commit_offsets
          @consumer.stop
          @consumer_thread.join
        end
      end # class Consumer
    end # module Kafka
  end # module Messaging
end # module Mimi
