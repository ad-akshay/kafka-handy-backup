import cbor2, bz2
from utils import KafkaMessage

AVAILABLE_ENCODERS = ['cbor']
AVAILABLE_COMPRESSORS = ['bz2']

class Encoder:

    def __init__(self, encoding = 'cbor', compression = None):
        self.encoder = encoding
        self.compression = compression

    def encode(self, msg):
        """Encode the message into a byte array"""
        if self.encoder == 'cbor':
            # Create an object with all the message details we need to save
            obj_msg = {
                # We use single letters keys to reduce the number of resulting bytes
                'v': msg.value(),
                'o': msg.offset(),
                'k': msg.key(),
                't': msg.timestamp()[1],
                'h': msg.headers()
            }

            return cbor2.dumps(obj_msg)

    def decode(self, bytes) -> KafkaMessage:
        if self.encoder == 'cbor':
            obj_msg = cbor2.loads(bytes)

            return KafkaMessage(
                topic=None,             # Unknown in this scope
                partition=None,         # Unknown in this scope
                value=obj_msg['v'],
                key=obj_msg['k'],
                headers=[ tuple(x) for x in obj_msg['h'] ] if obj_msg['h'] is not None else None,
                offset=obj_msg['o'],
                timestamp=obj_msg['t']
            )


    def compress(self, bytes) -> bytes:
        """Compress the given byte array"""
        if self.compression == 'bz2':
            return bz2.compress(bytes)
        else:
            return bytes

    def decompress(self, bytes) -> bytes:
        if self.compression == 'bz2':
            return bz2.decompress(bytes)
        else:
            return bytes

    def encode_message(self, msg):
        """Encodes a message into an optionally compressed and encrypted byte array"""
        return self.compress(self.encode(msg))

    def decode_message(self, bytes):
        return self.decode(self.decompress(bytes))
