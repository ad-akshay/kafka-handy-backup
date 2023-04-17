
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

AVAILABLE_ENCRYPTIONS = ['AES256']


class Encryptor:
    """
    Class handling encryption and decryption of data
    """

    def __init__(self, key: bytes, iv: bytes = None):
        self.encryption = 'AES256'
        self.iv = iv or os.urandom(16) # Generate if required
        self.cipher = Cipher(algorithms.AES(key), modes.CTR(self.iv))
        self.encryptor = self.cipher.encryptor()
        self.decryptor = self.cipher.decryptor()

    def encrypt(self, data: bytes) -> bytes:
        return self.encryptor.update(data)

    def decrypt(self, data: bytes) -> bytes:
        return self.decryptor.update(data)
