"""
Interface to the storage system
"""

from dataclasses import asdict
import json
import os
from struct import *
from time import strftime
from typing import List
import cbor2
from Encoder import Encoder
from FileStream import Encryptor, FileStream
from utils import Metadata

class Storage:

    streams = {}

    def __init__(self, base_path: str, max_chunk_size: int, encoder: Encoder, encryption_key: str):
        print(f'Configuring storage (base_path={base_path} max_chunk_size={max_chunk_size})')
        self.base_path = base_path
        self.max_chunk_size = max_chunk_size
        self.encoder = encoder
        self.encryption_key = encryption_key

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
            stream = FileStream(backup_directory=self.base_path, path=path)
            self.streams[stream_id] = stream

            # If encryption is configured, create an set the encryptor for that stream
            if self.encryption_key:
                stream.encryptor = Encryptor(key=self.encryption_key)

            # Write file header
            header = { 
                'encoding': 'cbor',                 # How the messages are encoded
                'offset': msg.offset(),             # Min offset
                'timestamp': msg.timestamp()[1],    # Min timestamp
                'compression': self.encoder.compression
            }

            # Add encryption details
            if stream.encryptor:
                header['encryption'] = stream.encryptor.encryption  # Type of encryption
                header['iv'] = stream.encryptor.iv                  # Initialization vector

            # Write file header (unencrypted because we need the IV to decrypt)
            cbor_header = cbor2.dumps(header) # Encode to CBOR
            stream.write(pack('<H', len(cbor_header)), disable_encryption=True)       # Header size (uint16, le)
            stream.write(cbor_header, disable_encryption=True)                        # Header content

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



    #
    #   Read functions
    #

    def list_restoration_points(self, limit) -> List[int]:
        """Returns the list of restoration points"""
        
        # Local file system
        metadata_path = self.base_path + '/metadata'
        metadata_files = [f for f in os.listdir(metadata_path) if os.path.isfile(os.path.join(metadata_path, f))]
        metadata_files.sort(reverse=True) # Latest first

        if limit is not None and limit > 0:
            metadata_files = metadata_files[0:limit]

        return [int(f.split('_')[0]) for f in metadata_files]

        # Object storage
        # TODO



from datetime import datetime

if __name__ == '__main__':
    
    # List the 
    os.listdir('backup/topics')

    metadata_path = 'backup/metadata'
    metadata_files = [f for f in os.listdir(metadata_path) if os.path.isfile(os.path.join(metadata_path, f))]
    metadata_files.sort(reverse=True) # Latest first

    restoration_points = []
    for f in metadata_files:
        epoch = int(f.split('_')[0])
        dt = datetime.fromtimestamp(epoch)
        print(f'{epoch} ({dt.strftime("%Y-%m-%d %H:%M:%S")})')
        restoration_points.append({
            'id': epoch,
            'datetime': dt.strftime("%Y-%m-%d %H:%M:%S")
        })

    print(metadata_files)
