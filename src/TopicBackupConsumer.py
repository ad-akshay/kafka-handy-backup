#
# Kafka consumer that backups the topic messages
#

import signal
from confluent_kafka import Consumer, TopicPartition
from Storage import Storage
from WritableMessageStream import WritableMessageStream
from utils import offsetToStr
from struct import *

KAFKA_BACKUP_CONSUMER_GROUP = 'kafka-backup-topic' # The consumer group used to read the topics

class TopicBackupConsumer:
    """Class used to backup all the partitions of a specified topic"""

    _exit_task = False
    consumer = None
    assigned_partitions = -1
    completed_partitions = 0
    continuous = False

    def __init__(self, storage: Storage, bootstrap_servers: str, topic: str, stop_offsets: dict[int, int], continuous=False):
        self.continuous = continuous
        self.storage = storage
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.stop_offsets = stop_offsets
        self.streams: dict[int, WritableMessageStream] = {}


    def _get_stream(self, partition: int) -> WritableMessageStream:
        if partition not in self.streams:
            self.streams[partition] = self.storage.get_writable_msg_stream(self.topic, partition)
        return self.streams[partition]

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
        self._exit_task = True

    def start(self, continuous=False):
        """Start the bakup process
            @param `stop_offsets` : { <partition> : <last_offset_to_backup> }
        """
        self.continuous = continuous

        self.consumer = Consumer({
            'group.id': KAFKA_BACKUP_CONSUMER_GROUP,
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'smallest', # Which offset to start if there are not committed offset
            'enable.auto.commit': False,
            'enable.auto.offset.store': False
        })

        print('Subscribing to:', self.topic)
        self.consumer.subscribe([self.topic], on_assign=self.on_assign, on_revoke=self.on_revoke, on_lost=self.on_lost)

        while True:
            messages = self.consumer.consume(timeout=5) # Get messages in batch

            if self._exit_task:
                break

            if len(messages) == 0: # No new messages
                if self.is_backup_completed():
                    print(f'Backup completed for topic {self.topic}')
                    break # Exit the loop

            for m in messages:
                if m.error():
                    print('Message error:', m.error())
                else:
                    if not self.continuous:
                        # The maxOffset if defined is our backup stop point
                        maxOffset = self.stop_offsets.get(m.partition())
                        # print(f'maxOffset={maxOffset} m.offset={m.offset()}')
                        if m.offset() >= (maxOffset - 1):
                            # We reached the stop offset for this partition
                            self.consumer.pause([TopicPartition(m.topic(), m.partition())]) # Stop consuming from this partition
                            print(f'Finished backing up {m.topic()}/{m.partition()}')
                            if m.offset() > maxOffset:
                                continue # Ignore messages in the batch that are above the max offset

                    self._get_stream(m.partition()).write_message(m)
                    self.consumer.store_offsets(message=m) # Will be commit later

            self.consumer.commit() # Commit stored offsets

        # Close all remaining streams
        for s in self.streams.values():
            s.close()

        self.consumer.close()

    def is_backup_completed(self) -> bool:
        """Returns True if the backup is complete, False otherwise."""
        # The backup is considered completed when the consumer committed offset
        # matches the specified stop offset.
        for partition_id in self.stop_offsets:
            maxOffset = self.stop_offsets[partition_id]
            partition = self.consumer.committed([TopicPartition(self.topic, partition_id)])
            print(f'{self.topic}/{partition_id} : committed={partition[0].offset} max={maxOffset}')
            if maxOffset != 0 and maxOffset > partition[0].offset:
                return False
        return True


