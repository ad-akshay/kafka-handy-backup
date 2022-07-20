"""
Stream for writing data to the local files system
"""
from faulthandler import disable
import os
from struct import *
from Encryptor import Encryptor


class FileStream:

    encryptor : Encryptor = None

    def __init__(self, backup_directory , path):
        self.filePath = backup_directory + '/' + path
        print(f'Creating {self.filePath}')
        os.makedirs(os.path.dirname(self.filePath), exist_ok=True) # Create missing directories
        self._size = 0
        self.file = open(self.filePath , 'wb')

    def write(self, bytes, disable_encryption=False):
        if self.encryptor and not disable_encryption:
            bytes = self.encryptor.encrypt(bytes)
        written = self.file.write(bytes)
        self._size += written
        return written

    def size(self):
        return self._size

    def close(self):
        print('Closing', self.filePath)
        # if self.encryptor:
        #     self.write(self.encryptor.finalize())
        
        self.file.close()
    