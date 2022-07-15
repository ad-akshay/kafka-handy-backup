"""
Interface to the storage system
"""

from dataclasses import asdict
import json
from struct import *
import cbor2
from Encoder import Encoder
from FileStream import FileStream
from utils import Metadata

class Storage:

    streams = {}

    def __init__(self, base_path, max_chunk_size, encoder):
        print(f'Configuring storage (base_path={base_path} max_chunk_size={max_chunk_size})')
        self.base_path = base_path
        self.max_chunk_size = max_chunk_size
        self.encoder = encoder

    def backup_message(self, msg):
        """Back up the given message to the proper stream (based on its topic and partition)"""
        
        # Target stream for this message (topic + partition)
        stream_id = f"{msg.topic()}/{msg.partition()}"
        
        # Get the stream (or create it if needed)
        stream = self.streams.get(stream_id, self.create_stream(stream_id, msg))
        
        # Encode the message
        encoded_msg = self.encoder.encode_message(msg)
        length = len(encoded_msg)

        # Make sure we would not exceed the max_chunk_size
        if stream.size() + length > self.max_chunk_size:
            # We would exceed, so create a new stream for this topic/partition
            print(f'Closing {stream.file.name} (reaching max chunk size)')
            self.close_stream(stream_id)
            stream = self.create_stream(stream_id, msg)

        # Write the encoded message to the stream : | size (uint32) | encoded_msg ([size] bytes) |
        # print(f'Writing message {msg.offset()} to {stream_id}')
        stream.write(pack('<H', length))
        stream.write(encoded_msg)

    def create_stream(self, stream_id, msg):
        """Create a new stream"""
        if not self.streams.get(stream_id):
            path = f"topics/{msg.topic()}/{msg.partition()}/{msg.offset()}_{msg.timestamp()[1]}"
            stream = FileStream(self.base_path, path)
            self.streams[stream_id] = stream

            # Write file header
            header = cbor2.dumps({ 
                'encoding': 'cbor',                 # How the messages are encoded
                'offset': msg.offset(),             # Min offset
                'timestamp': msg.timestamp()[1],    # Min timestamp
                'compression': self.encoder.compression
            })

            # print('Writing files header')
            # Write file header (info about the contained data)
            stream.write(pack('<H', len(header)))       # Header size (uint16, le)
            stream.write(header)                        # Header content (CBOR)

            return stream

    def backup_metadata(self, metadata: Metadata):
        """Backup the metadata to a file"""
        path = f'metadata/{metadata.timestamp}'
        file = FileStream(self.base_path, path)
        data = json.dumps(asdict(metadata)).encode()
        file.write(data)
        file.close()

    def close_stream(self, stream_id):
        if stream_id in self.streams:
            self.streams[stream_id].close()
            self.streams[stream_id] = None

    def close(self):
        for stream_id in self.streams:
            self.close_stream(stream_id)