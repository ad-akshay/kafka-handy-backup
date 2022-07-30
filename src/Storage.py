"""
Interface to the storage system
"""

from dataclasses import asdict
import os, cbor2
from struct import *
from Encoder import Encoder
from FileStream import Encryptor, FileStream
from ReadableMessageStream import ReadableMessageStream
from WritableMessageStream import WritableMessageStream
from utils import MetaData, key_id

class Storage:
    """
    The Storage class is an interface used to read and write to the backend storage.
    It can be used to probe some information about the backed-up data and also generate
    (Writable|Readable)MessageStreams that are used to read and write messages to/from
    the storage backend.
    """

    def __init__(self, base_path: str, max_chunk_size: int, encoder: Encoder, encryption_key: str, decryption_keys: list[bytes] = []):
        # print(f'Configuring storage (base_path="{base_path}", max_chunk_size={max_chunk_size})')
        self.base_path = base_path
        self.max_chunk_size = max_chunk_size
        self.encoder = encoder
        self.encryption_key = encryption_key
        self.decryption_keys = { key_id(x):x for x in decryption_keys }

    def backup_metadata(self, metadata: MetaData):
        """Save the given metadata to a file (creates a restoration point)"""
        path = f'{self.base_path}/metadata/{metadata.timestamp}'
        file = FileStream(path)
        data = cbor2.dumps(asdict(metadata))
        file.write(data)
        file.close()

    def available_restoration_points(self, limit) -> list[int]:
        """Returns the list of available restoration points"""
        
        # Local file system
        metadata_path = self.base_path + '/metadata'
        metadata_files = [f for f in os.listdir(metadata_path) if os.path.isfile(os.path.join(metadata_path, f))]
        metadata_files.sort(reverse=True) # Latest first

        if limit is not None and limit > 0:
            metadata_files = metadata_files[0:limit]

        return [int(f) for f in metadata_files]

        # Object storage
        # TODO

    def get_metadata(self, restoration_point_id: str = None) -> MetaData:
        """Return the metadata for the specified restoration point"""

        if restoration_point_id is None:
            # Default to latest restoration point
            rp = self.available_restoration_points(limit=1)
            if len(rp) == 0:
                return None
            restoration_point_id = str(rp[0])
        
        # Local file system
        metadata_file_path = self.base_path + '/metadata/' + restoration_point_id
        if not os.path.exists(metadata_file_path):
            print(f'ERROR: Restoration point {metadata_file_path} not found')
            return None
        
        with open(metadata_file_path, 'rb') as f:
            metadata = MetaData.fromObj(cbor2.loads(f.read()))

        return metadata

        # Object storage
        # TODO

    def available_topics(self):
        """Return a list of available (backed up) topics"""

        # Local file system
        topics_path = self.base_path + '/topics'
        if not os.path.exists(topics_path):
            print(f'Topics path "{topics_path} does not exist')
        available_topics = os.listdir(topics_path)
        return available_topics

    def list_chunks(self, topic, partition) -> list[str]:
        """Return the list of backup files/objects that contain data for a topic partition"""
        files = os.listdir(f'{self.base_path}/topics/{topic}/{partition}')
        files.sort()
        return [f'{self.base_path}/topics/{topic}/{partition}/{f}' for f in files]

    def get_chunk(self, topic, partition, offset):
        """Return the name of the chunk that contains a specific offset for a topic partition"""
        chunks = self.list_chunks(topic, partition)
        for c in chunks:
            minOffset = int(c.split('_')[0])
            if minOffset >= offset:
                return c

    def get_readable_msg_stream(self, topic: str, partition: int) -> ReadableMessageStream:
        """Return a ReadableMessageStream that can be used to read backed-up messages"""
        return ReadableMessageStream(
            topic,
            partition,
            self.decryption_keys,
            self.list_chunks(topic, partition)
        )

    def get_writable_msg_stream(self, topic: str, partition: int) -> WritableMessageStream:
        """Return a WritableMessageStream that can be used to backup messages for a topic partition"""
        return WritableMessageStream(
            topic,
            partition,
            self.encryption_key,
            self.max_chunk_size,
            self.base_path,
            self.encoder
        )