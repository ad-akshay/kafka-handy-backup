"""
Interface to the storage system
"""

from dataclasses import asdict
import logging
import os, cbor2
from struct import *
from Encoder import Encoder
from FileStream import FileStream
from ReadableMessageStream import ReadableMessageStream
from WritableMessageStream import WritableMessageStream
from utils import MetaData, key_id
from obs_client import *


logger = logging.getLogger(__name__)

class Storage:
    """
    The Storage class is an interface used to read and write to the backend storage.
    It can be used to probe some information about the backed-up data and also generate
    (Writable|Readable)MessageStreams that are used to read and write messages to/from
    the storage backend.
    """

    object_storage_client: ObjectStorageClient = None

    def __init__(self, base_path: str, max_chunk_size: int, encoder: Encoder, encryption_key: str, decryption_keys: list[bytes] = [], swift_url: str = None):
        # print(f'Configuring storage (base_path="{base_path}", max_chunk_size={max_chunk_size})')
        self.base_path = base_path
        self.max_chunk_size = max_chunk_size
        self.encoder = encoder
        self.encryption_key = encryption_key
        self.decryption_keys = { key_id(x):x for x in decryption_keys }

        if swift_url:
            self.object_storage_client = SwiftClient(swift_url)
            if self.object_storage_client.container_info(self.base_path) is None: # Container does not exist
                if not self.object_storage_client.container_create(self.base_path): # Can't create the container
                    logger.error(f'ERROR: Could not create container {self.base_path} on storage backend')

    def backup_metadata(self, metadata: MetaData):
        """Save the given metadata to a file (creates a restoration point)"""
        logger.info(f'Creating restoration point at {metadata.timestamp}')
        path = f'{self.base_path}/metadata/{metadata.timestamp}'
        file = FileStream(self.object_storage_client).open(path, 'write')
        data = cbor2.dumps(asdict(metadata))
        file.write(data)
        file.close()

    def available_restoration_points(self, limit) -> list[int]:
        """Returns the list of available restoration points"""
        if self.object_storage_client:
            # Object storage
            objects = self.object_storage_client.object_list(container_name=self.base_path, prefix='metadata/')
            metadata_files = [o.name.split('/')[1] for o in objects]
        else:
            # Local file system
            metadata_path = self.base_path + '/metadata'
            metadata_files = [f for f in os.listdir(metadata_path) if os.path.isfile(os.path.join(metadata_path, f))]
        
        metadata_files.sort(reverse=True) # Latest first

        if limit is not None and limit > 0:
            metadata_files = metadata_files[0:limit]

        return [int(f) for f in metadata_files]

    def get_metadata(self, restoration_point_id: str = None) -> MetaData:
        """Return the metadata for the specified restoration point"""
        
        if restoration_point_id is None:
            # Default to latest restoration point
            rp = self.available_restoration_points(limit=1)
            if len(rp) == 0:
                return None
            restoration_point_id = str(rp[0])

        metadata_file_path = self.base_path + '/metadata/' + restoration_point_id
        file = FileStream(self.object_storage_client).open(path=metadata_file_path, mode='read')
        if file:
            metadata = MetaData.fromObj(cbor2.loads(file.read()))
            file.close()
        else:
            print(f'ERROR: Restoration point {metadata_file_path} not found')
            return None

        return metadata

    def available_topics(self):
        """Return a list of available (backed up) topics"""

        if self.object_storage_client:
            objects = self.object_storage_client.object_list(container_name=self.base_path, prefix='topics/', delimiter='/')
            available_topics = [ t.subdir.split('/')[1] for t in objects if (isinstance(t, SubdirInfo)) ]
            return available_topics
        else:
            # Local file system
            topics_path = self.base_path + '/topics'
            if not os.path.exists(topics_path):
                print(f'Topics path "{topics_path} does not exist')
                return []
            available_topics = os.listdir(topics_path)
            return available_topics

    def list_chunks(self, topic, partition) -> list[str]:
        """Return the list of backup files/objects that contain data for a topic partition"""

        if self.object_storage_client:
            objects = self.object_storage_client.object_list(container_name=self.base_path, prefix=f'topics/{topic}/{partition}')
            chunks = [ f"{self.base_path}/{o.name}" for o in objects ]
            chunks.sort()
            return chunks
        else:
            # Local file system
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
            self.list_chunks(topic, partition),
            self.object_storage_client
        )

    def get_writable_msg_stream(self, topic: str, partition: int) -> WritableMessageStream:
        """Return a WritableMessageStream that can be used to backup messages for a topic partition"""
        return WritableMessageStream(
            topic,
            partition,
            self.encryption_key,
            self.max_chunk_size,
            self.base_path,
            self.encoder,
            self.object_storage_client
        )