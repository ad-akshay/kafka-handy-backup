import cbor2

AVAILABLE_ENCODERS = ['cbor']

class Encoder:

    def __init__(self, encoder = 'cbor'):
        self.encoder = encoder

        if self.encoder not in AVAILABLE_ENCODERS:
            print(f'ERROR: Unknown encoder "{encoder}"')

    def encode_message(self, msg):
        """Encodes a message. Returns a byte array."""

        if self.encoder == 'cbor':
            # Create an object with all the message details we need to save
            obj_msg = {
                'value': msg.value(),
                'offset': msg.offset(),
                'key': msg.key(),
                'timestamp': msg.timestamp()[1],
                'headers': msg.headers()
            }

            return cbor2.dumps(obj_msg)
