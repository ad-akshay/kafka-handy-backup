import cbor2
from struct import pack
from Encoder import Encoder
from Encryptor import Encryptor
from FileStream import FileStream
from confluent_kafka import Message
from obs_client import ObjectStorageClient
from utils import key_id

class WritableMessageStream:
    """
    WritableMessageStream is an abstraction interface that can be used as a stream to write messages to the storage backend.
    It automatically split the data into chunks so the caller does not have to handle that.
    """

    def __init__(self, topic: str, partition: int, encryption_key: str, max_chunk_size: int, base_path: str, encoder: Encoder, object_storage_client: ObjectStorageClient = None):
        self.topic = topic
        self.partition = partition
        self.encryption_key = encryption_key
        self.file = None
        self.max_chunk_size = max_chunk_size
        self.base_path = base_path
        self.encoder = encoder
        self.object_storage_client = object_storage_client

    def __new_chunk(self, msg: Message):
        """Create a new chunk of data"""
        path = f"{self.base_path}/topics/{msg.topic()}/{msg.partition()}/{msg.offset()}_{msg.timestamp()[1]}"
        self.file = FileStream(self.object_storage_client).open(path, mode='write')

        # If encryption is configured, create an set the encryptor for that stream
        if self.encryption_key:
            self.file.encryptor = Encryptor(key=self.encryption_key)

        # Write file header
        header = { 
            'encoding': 'cbor',                 # How the messages are encoded
            'offset': msg.offset(),             # Min offset
            'timestamp': msg.timestamp()[1],    # Min timestamp
            'compression': self.encoder.compression
        }

        # Add encryption details
        if self.file.encryptor:
            header['encryption'] = self.file.encryptor.encryption          # Type of encryption
            header['iv'] = self.file.encryptor.iv                          # Initialization vector
            header['key-id'] = key_id(self.encryption_key)                 # Store the encryption key id to identify the proper key during decryption

        # Write file header (unencrypted because we need the IV to decrypt)
        cbor_header = cbor2.dumps(header) # Encode to CBOR
        self.file.write(pack('<H', len(cbor_header)), disable_encryption=True)       # Header size (uint16, le)
        self.file.write(cbor_header, disable_encryption=True)                        # Header content


    def write_message(self, msg):
        if msg.topic() != self.topic or msg.partition() != self.partition:
            print('ERROR: Wrong topic-partition')
            return False

        if self.file is None:
            self.__new_chunk(msg)

        # Encode the message
        encoded_msg = self.encoder.encode_message(msg)
        length = len(encoded_msg)

        # Make sure we would not exceed the max_chunk_size
        if self.file.size() + length > self.max_chunk_size:
            # We would exceed, so create a new stream for this topic/partition
            print(f'Closing {self.file.file.name} (reaching max chunk size)')
            self.file.close()
            self.__new_chunk(msg)

        # Write the encoded message to the stream : | size (uint32) | encoded_msg ([size] bytes) |
        # print(f'Writing message {msg.offset()} to {stream_id}')
        self.file.write(pack('<H', length))
        self.file.write(encoded_msg)

    def close(self):
        self.file.close()