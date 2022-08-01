#
#   Class used to read the backed-up messages
#


import logging
from struct import unpack
from typing import Dict, List
from Encoder import Encoder
from Encryptor import Encryptor
from FileStream import FileStream
import cbor2
from confluent_kafka import Message

from utils import KafkaMessage
from universal_osc import ObjectStorageClient

logger = logging.getLogger(__name__)

class ReadableMessageStream:

    def __init__(self, topic: str, partition: int, decryption_keys: Dict, chunk_list: List[str], object_storage_client: ObjectStorageClient = None):
        self.object_storage_client = object_storage_client
        self.topic = topic
        self.partition = partition
        self.decryption_keys = decryption_keys
        self.chunks = chunk_list
        self.file = None
        self.next_offset = None # Holds the offset of the next message to be read

    def _get_chunk_name(self, offset: int = None) -> str|None:
        if offset is None and len(self.chunks) > 0:
            return self.chunks[0]
        else:
            for c in self.chunks:
                minOffset = int(c.split('/')[-1].split('_')[0]) # <backup-path>/<topics/<topic>/<partition>/<offset>_<timestamp>
                if minOffset >= offset:
                    return c
        
        return None

    def _load_chunk(self, offset: int = None) -> bool:
        """Load the chunk that contains the specified offset (default to first available chunk if offset is not specified)"""
        # Find the chunk that contains the offset
        if self.file is not None:
            self.file.close()
            self.file = None

        chunk_name = self._get_chunk_name(offset)
        if chunk_name is None:
            logger.error(f'ERROR: Could not find chunk for offset {offset}. Chunk does not seem to exist. Is this topic backed up ?')
            return False # Could not load chunk

        self.file = FileStream(self.object_storage_client).open(chunk_name, mode='read')

        logger.debug(f'Loading chunk {chunk_name} (size: {self.file.size()} bytes)')
        # Read header (header is not encrypted)
        tmp = self.file.read(2, disable_decryption=True)
        header_size = unpack('<H', tmp)[0]
        header_cbor = self.file.read(header_size, disable_decryption=True) # CBOR encoded header
        
        try:
            header : Dict = cbor2.loads(header_cbor)
        except:
            logger.error(f'ERROR: Invalid CBOR header for chunk {chunk_name}. Chunk seems corrupted.')
            self.file.close()
            self.file = None
            return False

        # Configure encryption
        if header.get('encryption') is not None:
            keyId = header.get('key-id')
            iv = header.get('iv')
            key = self.decryption_keys.get(keyId)
            if not key:
                logger.error(f'ERROR: Encryption key not found for chunk {chunk_name} (key-id={keyId}). Add encryption keys with the --encryption-key option.')
                self.file.close()
                self.file = None
                return False
            
            self.file.encryptor = Encryptor(key=key, iv=iv)

        # Configure encoding
        self.encoder = Encoder(header.get('encoding'), header.get('compression'))
        self.next_offset = header.get('offset')

        return True

    def seek(self, offset) -> bool:
        """Move the iterator to the given offset"""
        if not self._load_chunk(offset):
            return False

        while self.next_offset > offset:
            if self.next_message() is None:
                return False

        return True

    def next_message(self) -> KafkaMessage:
        if self.file is None:
            return None

        if self.file.at_end():
            self._load_chunk(self.next_offset)

        # Read message size
        msg_size = unpack('<H', self.file.read(2))[0]
        msg_encoded = self.file.read(msg_size)
        msg = self.encoder.decode_message(msg_encoded)
        msg.topic = self.topic
        msg.partition = self.partition
        self.next_offset = msg.offset + 1
        return msg

    def __iter__(self):
        return self

    def __next__(self):
        return self.next_message()