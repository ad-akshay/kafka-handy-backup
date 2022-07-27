#
#   Class that handles the restoration of a single topic-partition
#


import threading
from typing import List
from Storage import Storage
from confluent_kafka import Producer


from utils import key_id

class TopicRestorationProducer():

    _exit_signal = False

    def __init__(self, src_topic: str, dst_topic: str, partition: int, original_partitions: bool, minOffset:int, maxOffset:int, storage: Storage, bootstrap_server: str):
        """
            src_topic
                Source topic name

            dst_topic
                Destination topic name

            partition
                Partition to restore (from the backup)

            original_partitions
                If True, publish messages to the same partition number

            encryption_keys
                List of encryption keys to use for decrypting the data

            minOffset
                First offset to backup

            maxOffset
                Stop backup at this offset (maxOffset - 1 is the last offset that will be backed up)
        """
        self.storage = storage
        self.dst_topic = dst_topic
        self.src_topic = src_topic
        self.partition = partition
        self.original_partitions = original_partitions
        self.offset_start = minOffset   # First offset to restore
        self.offset_stop = maxOffset    # Last offset to restore (actually maxOffset - 1 is the max to restore)
        self.bootstrap_server = bootstrap_server

        self.cursor = self.offset_start

    def start(self):
        print(f'Starting restoration for {self.src_topic}/{self.partition}')

        self.msg_stream = self.storage.get_readable_msg_stream(self.src_topic, self.partition)
        if not self.msg_stream.load_chunk(self.offset_start):
            print(f'ERROR: Could not load chunk for topic {self.src_topic}/{self.partition} offset {self.offset_start}. Is this offset backed up ?')
            return

        # Create a producer
        producer = Producer({
            'bootstrap.servers': self.bootstrap_server,
            'client.id': 'kafka-backup'
        })

        for msg in self.msg_stream:
            # print("Restoring", msg)

            if self._exit_signal:
                break

            if msg.offset+1 >= self.offset_stop:
                break

            # Publish the messages
            producer.produce(
                topic=self.dst_topic,
                key=msg.key,
                value=msg.value,
                partition=msg.partition if self.original_partitions else -1,
                timestamp=msg.timestamp,
                headers=msg.headers,
                # on_delivery= lambda x, y: print('on_delivery', x, str(y))
                )
            producer.flush()

        print(f'Restoration of {self.src_topic}/{self.partition} completed up to offset {msg.offset}')


    def cancel(self):
        self._exit_signal = True