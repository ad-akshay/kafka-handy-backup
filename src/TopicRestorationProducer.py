#
#   Class that handles the restoration of a single topic-partition
#


import threading
from typing import List
from Storage import Storage

from utils import key_id

class TopicRestorationProducer():

    _exit_signal = False

    def __init__(self, src_topic: str, dst_topic: str, partition: int, original_partitions: bool, encryption_keys: List[bytes], minOffset:int, maxOffset:int, storage: Storage):
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

        self.encryption_keys = { key_id(x):x for x in encryption_keys } # Create a dict { <key_id>:<key_value> }
        
        self.cursor = self.offset_start

    def start(self):
        print(f'Starting restoration for {self.src_topic}/{self.partition}')

        # Create the list of all chunks we need for restoration
        self.chunks = self.storage.list_chunks(self.src_topic, self.partition)

        first_index = None
        last_index = None
        for i in range(0, len(self.chunks)):
            file_first_offset = int(self.chunks[i].split('_')[0])
            if first_index is None and file_first_offset <= self.offset_start: first_index = i
            if last_index is None and file_first_offset > self.offset_stop: last_index = i

        self.chunks = self.chunks[first_index:last_index]

        # print('Chunks', self.chunks)

        # Create a producer

        # For each chunk, decode and publish the messages
        # for c in self.chunks:
            # Read the file content

            # If offset_stop is reached, stop publishing and exit the task


        # Check the exit signal
        
        
        print(f'Restoration of {self.src_topic}/{self.partition} completed')


    def cancel(self):
        self._exit_signal = True