import time
import hmac
import hashlib
import base64
from typing import Dict


class ExchangeBitcoinAuth:
    """
    Auth class required by Exchange Bitcoin API
    """
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def generate_auth_dict(self, method: str, path_url: str, body: str = "") -> Dict[str, any]:
        """
        Generates authentication signature and return it in a dictionary along with other inputs
        :return: a dictionary of request info including the request signature
        """
        raise NotImplementedError