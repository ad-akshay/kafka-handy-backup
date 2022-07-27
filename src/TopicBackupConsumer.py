#
# Kafka consumer that backups the topic messages
#

import signal
from typing import Dict, List
from confluent_kafka import Consumer, TopicPartition
from utils import offsetToStr
from struct import *


class TopicBackupConsumer:

    consumer = None
    exit_task = False
    assigned_partitions = -1
    completed_partitions = 0

    def __init__(self, storage, bootstrap_servers):
        self.storage = storage
        self.bootstrap_servers = bootstrap_servers

    # Consumer callbacks

    def on_assign(self, consumer, partitions):
        if self.assigned_partitions == -1: self.assigned_partitions = 0
        self.assigned_partitions = self.assigned_partitions + len(partitions)
        partitions = consumer.committed(partitions) # Get the real consumer offsets
        print('on_assign:', [f'{p.topic}:{p.partition} offset={offsetToStr(p.offset)}' for p in partitions])

    def on_revoke(self, consumer, partitions):
        self.assigned_partitions = self.assigned_partitions - len(partitions)
        self.completed_partitions = self.completed_partitions - len(partitions)
        print('on_revoke:', [f'{p.topic}:{p.partition}' for p in partitions])

    def on_lost(self, consumer, partitions):
        self.assigned_partitions = self.assigned_partitions - len(partitions)
        self.completed_partitions = self.completed_partitions - len(partitions)
        print('on_revoke:', [f'{p.topic}:{p.partition}' for p in partitions])

    # Tasks control

    def stop(self):
        self.exit_task = True

    def start(self, topics_to_backup: List[str], offsets, continuous=False, from_start=False):

        consumer = Consumer({
            'group.id': 'kafka-backup-topic',
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'smallest', # Which offset to start if there are not committed offset
            'enable.auto.commit': False
        })

        # Filter out the topics that have no new messages since last backup or are empty
        if continuous:
            topic_list = topics_to_backup
        else:
            topic_list = []
            for topic in topics_to_backup:
                for p in offsets[topic]:
                    maxOffset = offsets[topic][p]
                    partition = consumer.committed([TopicPartition(topic, p)])
                    print(f'{topic}/{p} : committed={partition[0].offset} max={maxOffset}')
                    if maxOffset != 0 and maxOffset > partition[0].offset:
                        topic_list.append(topic)
                        break

        if len(topic_list) == 0:
            print('All topics already backed up')
            consumer.close()
            return

        print('Subscribing to:', topic_list)
        consumer.subscribe(topic_list, on_assign=self.on_assign, on_revoke=self.on_revoke, on_lost=self.on_lost)

        while True:
            messages = consumer.consume(timeout=3) # Get messages in batch

            if self.exit_task:
                break

            # if len(messages) == 0:
            #     print(f'Timeout without messages (topic_count={topic_count} assigned_partitions={self.assigned_partitions})')

            for m in messages:
                if m.error():
                    print('Message error', m.error())
                else:
                    if not continuous:
                        # The maxOffset if defined is our backup stop point
                        maxOffset = offsets[m.topic()][m.partition()]
                        # print(f'maxOffset={maxOffset} m.offset={m.offset()}')
                        if maxOffset is not None and m.offset() >= (maxOffset - 1):
                            # Stop consuming from this partition
                            consumer.pause([TopicPartition(m.topic(), m.partition())])
                            self.completed_partitions = self.completed_partitions + 1
                            print(f'Finished backing up {m.topic()}:{m.partition()} (assigned={self.assigned_partitions} paused={self.completed_partitions})')
                            if m.offset() > maxOffset:
                                continue # Ignore messages in the batch that are above the max offset

                    self.storage.backup_message(m)

                    consumer.commit(message=m) # TODO: Use the offsets instead
                    # consumer.commit(offsets=[TopicPartition(m.topic(), m.partition(), m.offset())]) # TODO: Use the offset

            if not continuous and self.completed_partitions == self.assigned_partitions:
                # We are done backing up the topics
                print('All topics are backed up')
                break

        print('Stopping task')
        consumer.close()
        self.storage.close()


if __name__ == "__main__":
    # Capture interrupt to clean exit
    def signal_handler(sig, frame):
        global a
        a.stop()
    signal.signal(signal.SIGINT, signal_handler)

    a = TopicBackupConsumer(['test-topic-2', 'test-topic-1'])
    a.start()
