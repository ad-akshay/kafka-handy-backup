#
# Kafka consumer that backups the topic messages
#

import json
from mimetypes import init
import os
from pydoc_data.topics import topics
import signal, sys
from confluent_kafka import Consumer, TopicPartition
from utils import offsetToStr
from struct import *
from pathlib import Path


TOPIC_LIST = ['test-topic-2', 'test-topic-1']
BOOTSTRAP_SERVERS = 'localhost:29092'



class TopicBackupConsumer:

    consumer = None
    exit_task = False

    streams = {}

    def __init__(self, topics_to_backup = None):
        self.topics_list = topics_to_backup

    # Consumer callbacks

    def on_assign(self, consumer, partitions):
        partitions = consumer.committed(partitions) # Get the real offsets
        print('on_assign:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])

    def on_revoke(self, consumer, partitions):
        print('on_revoke:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])

    def on_lost(self, consumer, partitions):
        print('on_revoke:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])

    # Tasks control

    def stop(self):
        self.exit_task = True

    def start(self, topics_to_backup = None):

        if topics_to_backup != None:
            self.topics_list = topics_to_backup

        consumer = Consumer({
            'group.id': 'kafka-backup-topic',
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': False
        })

        consumer.subscribe(self.topics_list, on_assign=self.on_assign, on_revoke=self.on_revoke, on_lost=self.on_lost)

        while True:
            messages = consumer.consume(timeout=3) # Get messages in batch

            if self.exit_task:
                break

            # if len(messages) == 0:
            #     print('Timeout without messages')

            for m in messages:
                if m.error():
                    print('Message error', m.error())
                else:
                    # Valid message
                    # print(f'{m.topic()}:{m.partition()} {m.offset()}')
                    self.backup_message(m)

                    consumer.commit(message=m) # TODO: Use the offsets instead

                    # consumer.commit(offsets=[TopicPartition(m.topic(), m.partition(), m.offset())]) # TODO: Use the offset

        print('Closing consumer')
        consumer.close()
        print('Closing consumer - done')
        for stream in self.streams.values():
            stream.close()

    def backup_message(self, msg):

        # Stream in which this message should go
        stream_id = f"{msg.topic()}/{msg.partition()}"

        if stream_id not in self.streams:
            # The stream does not exist, lets create it
            path_prefix = 'backup/'
            path = f"{msg.topic()}/{msg.partition()}/{msg.offset()}_{msg.timestamp()[1]}"
            filePath = path_prefix + path
            print(f'Creating stream {filePath}')
            os.makedirs(os.path.dirname(filePath), exist_ok=True) # Create missing directories
            stream = open(filePath , 'wb')
            self.streams[stream_id] = stream

            # Write file header
            header = json.dumps({ 
                'encoding': 'json',
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1]
                # 'encryption' 'compression'
                }).encode()
            header_len = len(header)

            print('Writing files header')
            # Write file header (info about the contained data)
            stream.write(pack('<H', header_len))    # Header size (uint16, le)
            stream.write(header)                    # Header (CBOR)

        stream = self.streams[stream_id]
        
        # Encode the message
        obj_msg = {
            'value': msg.value().decode(),
            'offset': msg.offset(),
            'key': msg.key().decode(),
            'timestamp': msg.timestamp()[1],
            'headers': msg.headers()
        }

        encoded_msg = json.dumps(obj_msg).encode()

        # Write the message to the stream : | size (uint32) | encoded_msg ([size] bytes) |
        print(f'Writing message {msg.offset()} to {stream_id}')
        stream.write(pack('<H', len(encoded_msg)))
        stream.write(encoded_msg)


if __name__ == "__main__":
    # Capture interrupt to clean exit
    def signal_handler(sig, frame):
        global a
        a.stop()
    signal.signal(signal.SIGINT, signal_handler)

    a = TopicBackupConsumer(TOPIC_LIST)
    a.start()

    print('End of script')
