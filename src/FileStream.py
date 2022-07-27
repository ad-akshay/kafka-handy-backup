"""
Stream for writing data to the local files system
"""
from faulthandler import disable
import os
from struct import *
from Encryptor import Encryptor


class FileStream:

    encryptor : Encryptor = None

    def __init__(self , path, mode = 'write'):
        self.filePath = path
        os.makedirs(os.path.dirname(self.filePath), exist_ok=True) # Create missing directories
        self._size = 0

        if mode == 'read':
            print(f'Opening {self.filePath}')
            self.file = open(self.filePath, 'rb')
            self.file.seek(0, 2) # Go to end of file
            self._size = self.file.tell()
            self.file.seek(0)    # Back to start
        else:
            print(f'Creating {self.filePath}')
            self.file = open(self.filePath , 'wb')

    def write(self, bytes, disable_encryption=False):
        if self.encryptor and not disable_encryption:
            bytes = self.encryptor.encrypt(bytes)
        written = self.file.write(bytes)
        self._size += written
        return written

    def read(self, size=None, disable_decryption=False) -> bytes:
        bytes = self.file.read(size)
        if self.encryptor and not disable_decryption:
            bytes = self.encryptor.decrypt(bytes)
        return bytes

    def size(self):
        return self._size

    def at_end(self):
        return self.file.tell() == self.size()

    def close(self):
        print('Closing', self.filePath)
        # if self.encryptor:
        #     self.write(self.encryptor.finalize())
        
        self.file.close()
    