"""
Interface to the storage system
"""

from struct import *
import cbor2
from Encoder import Encoder
from FileStream import FileStream

class Storage:

    streams = {}

    def __init__(self, base_path):
        self.base_path = base_path

        self.encoder = Encoder('cbor')

    def backup_message(self, msg):
        """Back up the given message to the proper stream (based on its topic and partition)"""
        
        # Target stream for this message (topic + partition)
        stream_id = f"{msg.topic()}/{msg.partition()}"
        
        # Ensure stream exists
        if stream_id not in self.streams:
            self.create_stream(stream_id, msg)

        stream = self.streams[stream_id]
        
        # Encode the message
        encoded_msg = self.encoder.encode_message(msg)

        # if encoded_msg is None:
        #     print('ERROR encoding message')
        #     return

        # Write the encoded message to the stream : | size (uint32) | encoded_msg ([size] bytes) |
        print(f'Writing message {msg.offset()} to {stream_id}')
        stream.write(pack('<H', len(encoded_msg)))
        stream.write(encoded_msg)


    def create_stream(self, stream_id, msg):
        """Create a new stream"""
        if not self.streams.get(stream_id):
            path = f"{msg.topic()}/{msg.partition()}/{msg.offset()}_{msg.timestamp()[1]}"
            stream = FileStream(self.base_path, path)
            self.streams[stream_id] = stream

            # Write file header
            header = cbor2.dumps({ 
                'encoding': 'cbor',                 # How the messages are encoded
                'offset': msg.offset(),             # Min offset
                'timestamp': msg.timestamp()[1]     # Min timestamp
            })

            print('Writing files header')
            # Write file header (info about the contained data)
            stream.write(pack('<H', len(header)))       # Header size (uint16, le)
            stream.write(header)                        # Header content (CBOR)

    def close_stream(self, stream_id):
        if stream_id in self.streams:
            self.streams[stream_id].close()
            self.streams[stream_id] = None

    def close(self):
        for stream_id in self.streams:
            self.close_stream(stream_id)