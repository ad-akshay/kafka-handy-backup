#
# Kafka consumer that backups the topic messages
#

import signal, sys
from confluent_kafka import Consumer, TopicPartition

TOPIC_LIST = ['test-topic-2', 'test-topic-1']
BOOTSTRAP_SERVERS = 'localhost:29092'


exit_task = False
def signal_handler(sig, frame):
    global exit_task
    exit_task = True
    print('You pressed Ctrl+C!')

signal.signal(signal.SIGINT, signal_handler)

def backup_topics_task(topics_to_backup):

    # Create a consumer
    consumer = Consumer({
        'group.id': 'kafka-backup-topic',
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'auto.offset.reset': 'smallest',
        'enable.auto.commit': False
    })

    # Subscribe to the given topics
    consumer.subscribe(topics_to_backup)

    while True:
        messages = consumer.consume(timeout=10) # Get messages in batch

        if exit_task:
            break

        if len(messages) == 0:
            print('Timeout without messages')

        for m in messages:
            if m.error():
                print('Message error', m.error())
            else:
                # Valid message
                print(f'{m.topic()}:{m.partition()} {m.offset()}')

                # Write to the right backup file
                # filePath = f'{m.topic()}/{m.partition()}/{min_timestamp}'

                consumer.commit(message=m) # TODO: Use the offsets instead

                # consumer.commit(offsets=[TopicPartition(m.topic(), m.partition(), m.offset())]) # TODO: Use the offset

    consumer.close()


if __name__ == "__main__":
    backup_topics_task(TOPIC_LIST)

