#
#   Class that handles the restoration of a single topic-partition
#


from typing import Dict, List
from Storage import Storage
from confluent_kafka import Producer
import Metadata

from utils import ConsumerDetails, PartitionDetails, TopicDetails, setCommittedOffset

class TopicRestorationProducer():

    _exit_signal = False

    def __init__(self, src_topic: str, dst_topic: str, partition: int, original_partitions: bool, minOffset:int, maxOffset:int, storage: Storage, bootstrap_server: str, restore_consumer_offset: bool, consumer_offsets: Dict[str, ConsumerDetails]):
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
        self.restore_consumer_offsets = restore_consumer_offset
        self.consumer_offsets = consumer_offsets

    def start(self):
        print(f'Starting restoration for {self.src_topic}/{self.partition}')

        self.msg_stream = self.storage.get_readable_msg_stream(self.src_topic, self.partition)
        if not self.msg_stream.seek(self.offset_start):
            # print(f'ERROR: Could not load chunk for topic {self.src_topic}/{self.partition} offset {self.offset_start}.')
            return

        # Create a producer
        producer = Producer({
            'bootstrap.servers': self.bootstrap_server,
            'client.id': 'kafka-backup'
        })

        next_offset = self.offset_start # Which offset to restore next
        for msg in self.msg_stream:
            # print("Restoring", msg)
            # print(f'next_offset={next_offset} msg.offset={msg.offset} self.offset_stop={self.offset_stop}')
            
            if self._exit_signal:
                break

            if next_offset > msg.offset:
                continue # Not there yet

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

            next_offset += 1

            if msg.offset + 1 >= self.offset_stop:
                break

        producer.flush()

        # Restore consumer offsets
        if self.restore_consumer_offsets and self.original_partitions:
            print(f'Restoring consumer offsets for topic {self.src_topic}')
            
            # Get the current max offset of the topic
            topics_details = Metadata.topics_details(self.bootstrap_server)
            t = topics_details[self.dst_topic]
            p = t.partitions[self.partition]

            # Calculate the offset variation
            offset_variation = p.maxOffset - next_offset

            # For each consumer that have a committed offset for this topic-partition
            # print(self.consumer_offsets)
            for group in self.consumer_offsets:
                for co in self.consumer_offsets[group]:
                    # print(co)
                    if self.src_topic == co.topic and self.partition == co.partition:
                        # This offset applies to this topic partition

                        # Because the offsets in the destination topic might
                        # not be the same as the ones in the source topic (backup)
                        # we need to adjust the value with the variation
                        new_offset = co.committedOffset + offset_variation
                        
                        # Restore the committed offset
                        print(f'Committing offset: {self.dst_topic}/{self.partition} for {group} to {new_offset}')
                        setCommittedOffset(group, self.dst_topic, self.partition, new_offset, self.bootstrap_server)
        

        print(f'Restoration of {self.src_topic}/{self.partition} completed up to offset {msg.offset}')


    def cancel(self):
        self._exit_signal = True