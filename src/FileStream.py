"""
Stream for writing data to the local files system
"""
import os
from struct import *

class FileStream:

    def __init__(self, backup_directory , path):
        filePath = backup_directory + '/' + path
        print(f'Creating FileStream at {filePath}')
        os.makedirs(os.path.dirname(filePath), exist_ok=True) # Create missing directories
        self._size = 0
        self.file = open(filePath , 'wb')

    def write(self, bytes):
        self._size += self.file.write(bytes)

    def size(self):
        return self._size

    def close(self):
        self.file.close()
    