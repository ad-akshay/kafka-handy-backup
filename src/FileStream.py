"""
Stream for writing data to the local files system
"""
import os
from struct import *
import time
from Encryptor import Encryptor
import logging

from obs_client import ObjectStorageClient

logger = logging.getLogger(__name__)

class FileStream:

    encryptor: Encryptor = None
    client: ObjectStorageClient = None

    def __init__(self, object_storage_client: ObjectStorageClient = None):
        self.client = object_storage_client

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
        logging.debug(f'Closing {self.filePath}')
        self.file.close()
        
        if self.client: 
            # Object storage backend configured
            if self.mode == 'write':
                logging.info(f'Uploading {self.filePath} to object storage')
                start_time = time.time()
                ok = self.client.upload_file(self.filePath, self.filePath)
                logging.debug(f'Uploading {self.filePath} ({round(self.size()/1024/1024)} Mb) completed in {round(time.time() - start_time, 2)} seconds')
                if ok:
                    logging.debug(f'Deleting {self.filePath} from local file system')
                    os.remove(self.filePath) # We don't need the local file anymore
                else:
                    logger.error(f'ERROR: Could not upload file {self.filePath} to object storage')
                    # TODO: What should we do here ? Retry uploading later ? For now the file remains on the local file system.
            else:
                # Read mode, we can delete the file once we're done with it
                logging.debug(f'Deleting {self.filePath} from local file system')
                os.remove(self.filePath) # We don't need the local file anymore


    def open(self, path: str, mode: str = 'read'):
        self.filePath = path
        self.mode = mode
        logging.debug(f'Opening {self.filePath}')

        if mode == 'write':
            # Local file system
            logging.debug(f'Creating {self.filePath}')
            os.makedirs(os.path.dirname(self.filePath), exist_ok=True) # Create missing directories
            self.file = open(self.filePath , 'wb')
            self._size = 0
            return self
        else:
            if self.client:
                # Object storage is configured, we need to download the file first
                os.makedirs(os.path.dirname(self.filePath), exist_ok=True) # Make sure the target file system directory exist
                logger.debug(f'Downloading chunk {self.filePath}')
                ok = self.client.download_file(self.filePath, self.filePath)
                if not ok:
                    logger.error(f'ERROR downloading chunk {self.filePath}')

            if not os.path.exists(self.filePath):
                return None
            
            # Local file system
            self.file = open(self.filePath, 'rb')
            self.file.seek(0, 2) # Go to end of file
            self._size = self.file.tell()
            self.file.seek(0)    # Back to start
        
        return self
    