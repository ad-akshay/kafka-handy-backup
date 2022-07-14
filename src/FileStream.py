"""
Stream for writing data to local files
"""
import os
from struct import *
import cbor2


class FileStream:

    streams = {}

    def __init__(self, path_prefix = ''):
        self.path_prefix = ''

    def backup_message(self, msg):
        """Back up the given message to the proper stream (based on its topic and partition)"""
        
        # Target stream for this message (topic + partition)
        stream_id = f"{msg.topic()}/{msg.partition()}"
        
        # Ensure stream exists
        if stream_id not in self.streams:
            self.create_stream(stream_id, msg)

        stream = self.streams[stream_id]
        
        # Create an object with all the message details we need to save
        obj_msg = {
            'value': msg.value(),
            'offset': msg.offset(),
            'key': msg.key(),
            'timestamp': msg.timestamp()[1],
            'headers': msg.headers()
        }

        # Encode the message
        encoded_msg = cbor2.dumps(obj_msg)

        # Write the encoded message to the stream : | size (uint32) | encoded_msg ([size] bytes) |
        print(f'Writing message {msg.offset()} to {stream_id}')
        stream.write(pack('<H', len(encoded_msg)))
        stream.write(encoded_msg)


    def create_stream(self, stream_id, msg):
        """Create a new stream"""
        if stream_id not in self.streams:
            # The stream does not exist, lets create it
            path_prefix = 'backup/'
            path = f"{msg.topic()}/{msg.partition()}/{msg.offset()}_{msg.timestamp()[1]}"
            filePath = path_prefix + path
            print(f'Creating stream {filePath}')
            os.makedirs(os.path.dirname(filePath), exist_ok=True) # Create missing directories
            stream = open(filePath , 'wb')
            self.streams[stream_id] = stream

            # Write file header
            header = cbor2.dumps({ 
                'encoding': 'cbor',
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1]
                # 'encryption' 'compression'
                })
            header_len = len(header)

            print('Writing files header')
            # Write file header (info about the contained data)
            stream.write(pack('<H', header_len))    # Header size (uint16, le)
            stream.write(header)                    # Header (CBOR)

    def close(self):
        for s in self.streams.values():
            s.close()
            