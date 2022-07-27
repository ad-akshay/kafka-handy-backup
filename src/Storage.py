"""
Interface to the storage system
"""

from dataclasses import asdict
import json
import os
from struct import *
from time import strftime
from typing import Dict, List
import cbor2
from Encoder import Encoder
from FileStream import Encryptor, FileStream
from ReadableMessageStream import ReadableMessageStream
from utils import Metadata, key_id

class Storage:

    streams = {}

    def __init__(self, base_path: str, max_chunk_size: int, encoder: Encoder, encryption_key: str, decryption_keys: List[bytes] = []):
        print(f'Configuring storage (base_path="{base_path}", max_chunk_size={max_chunk_size})')
        self.base_path = base_path
        self.max_chunk_size = max_chunk_size
        self.encoder = encoder
        self.encryption_key = encryption_key
        self.decryption_keys = { key_id(x):x for x in decryption_keys }

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

        print(f"{stream_id} offset={msg.offset()}")

    def create_stream(self, stream_id, msg):
        """Create a new stream"""
        if not self.streams.get(stream_id):
            path = f"{self.base_path}/topics/{msg.topic()}/{msg.partition()}/{msg.offset()}_{msg.timestamp()[1]}"
            stream = FileStream(path)
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
                header['encryption'] = stream.encryptor.encryption          # Type of encryption
                header['iv'] = stream.encryptor.iv                          # Initialization vector
                header['key-id'] = key_id(self.encryption_key)              # Store the encryption key id to identify the proper key during decryption

            # Write file header (unencrypted because we need the IV to decrypt)
            cbor_header = cbor2.dumps(header) # Encode to CBOR
            stream.write(pack('<H', len(cbor_header)), disable_encryption=True)       # Header size (uint16, le)
            stream.write(cbor_header, disable_encryption=True)                        # Header content

            return stream

    def backup_metadata(self, metadata: Metadata):
        """Backup the metadata to a file"""
        path = f'{self.base_path}/metadata/{metadata.timestamp}'
        file = FileStream(path)
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

        return [int(f) for f in metadata_files]

        # Object storage
        # TODO

    def get_metadata(self, restoration_point_id: str = None) -> Metadata:
        """Return the metadata for the given restoration point"""

        if restoration_point_id is None:
            # Default to latest
            rp = self.list_restoration_points(limit=1)
            if len(rp) == 0:
                return None
            restoration_point_id = str(rp[0])
        
        # Local file system
        metadata_file_path = self.base_path + '/metadata/' + restoration_point_id
        if not os.path.exists(metadata_file_path):
            print(f'ERROR: Restoration point {metadata_file_path} not found')
            return None
        
        with open(metadata_file_path, 'rb') as f:
            metadata = Metadata(**json.loads(f.read()))

        return metadata

        # Object storage
        # TODO

    def list_available_topics(self):
        """Return a list of available (backed up) topics"""

        # Local file system
        topics_path = self.base_path + '/topics'
        if not os.path.exists(topics_path):
            print(f'Topics path "{topics_path} does not exist')
        available_topics = os.listdir(topics_path)
        return available_topics

    def list_chunks(self, topic, partition) -> List[str]:
        """Return the list of backup files that contain data for this topic partition"""
        files = os.listdir(f'{self.base_path}/topics/{topic}/{partition}')
        files.sort()
        return [f'{self.base_path}/topics/{topic}/{partition}/{f}' for f in files]

    def get_chunk(self, topic, partition, offset):
        """Return the name of the chunk that contains the given offset for a topic partition"""
        chunks = self.list_chunks(topic, partition)
        for c in chunks:
            minOffset = int(c.split('_')[0])
            if minOffset >= offset:
                return c

    def get_readable_msg_stream(self, topic, partition):
        return ReadableMessageStream(
            topic,
            partition,
            self.decryption_keys,
            self.list_chunks(topic, partition)
        )