from datetime import (
    datetime,
    timezone,
)

import base64
import json
import random
import string

from cryptography.fernet import Fernet
from jose import jwt
from passlib.context import CryptContext

from server.core.constants import Constants


class AuthUtility:
    """Utility methods when authenticating users."""

    def __init__(self):
        pass

    def authenticate_password(self, password, authentic_password):
        """Authenticates password from `web`.

        Args:
            password: User-submitted password
            authentic_password: Encrypted password associated to user account

        Returns:
            True if password is authentic
            False otherwise
        """
        context = CryptContext(
            schemes=[Constants.BCRYPT],
            deprecated=[Constants.AUTO],
        )
        return context.verify(
            password,
            authentic_password,
        )

    def decrypt_parameters(self, encrypted_parameters, key):
        """Decrypts parameters using key.

        Args:
            encrypted_parameters: Encrypted parameters
            key: Encryption key

        Returns:
            dict that contains decrypted parameters
        """
        b64_key = self._base64_decode(
            key.encode(Constants.UTF8),
        )
        
        cipher = Fernet(b64_key)
        decrypted_parameters = cipher.decrypt(
            encrypted_parameters.encode(Constants.UTF8) + b'==',
        )

        return json.loads(
            decrypted_parameters.decode(Constants.UTF8),
        )

    def encode_token(self, token, session_duration, secret_key):
        """Encodes token as JSON Web Token.

        Args:
            token: Token to encode
            session_duration: TTL of token
            secret_key: Secret to use when encoding JWT

        Returns:
            A tuaple of a JSON web token and its expiration timestamp
        """
        expiration = datetime.utcnow() + session_duration
        token.exp = int(
            expiration.replace(
                tzinfo=timezone.utc,
            ).timestamp()
        )

        return (
            jwt.encode(
                token.dict(),
                secret_key,
                algorithm=Constants.JWT_ALGORITHM,
            ),
            token.exp,
        )
    
    def encrypt_parameters(self, parameters, key):
        """Encrypts parameters using a key.

        Args:
            parameters: Dictionary of parameters
            key: Encryption key

        Returns:
            str of encrypted parameters
        """
        b64_key = self._base64_decode(
            key.encode(Constants.UTF8),
        )
        cipher = Fernet(b64_key)
        cipher_text = cipher.encrypt(
            json.dumps(parameters).encode(Constants.UTF8),
        )

        return cipher_text.decode(Constants.UTF8)

    def encrypt_password(self, password):
        """Encrypts a plain-text password.

        Args:
            password: Plain-text password

        Returns:
            Encrypted password
        """
        context = CryptContext(
            schemes=[Constants.BCRYPT],
            deprecated=[Constants.AUTO],
        )
        return context.hash(password)

    def password(self):
        """Generates a random password.
        
        This is used to create passwords when inviting users.

        Returns:
            Randomly-generated password
        """
        LETTERS = string.ascii_letters
        NUMBERS = string.digits

        password_text = f'{LETTERS}{NUMBERS}{Constants.SYMBOLS}'
        password_characters = list(password_text)
        random.shuffle(password_characters)

        password = random.choices(
            password_characters,
            k=Constants.PASSWORD_LENGTH - 3,
        )
        password = password + random.choices(Constants.SYMBOLS)
        password = password + random.choices(LETTERS)
        password = password + random.choices(NUMBERS)

        return Constants.EMPTY_STRING.join(password)

    def _base64_decode(self, value):
        """Adds missing padding to string and return the decoded base64 string.
        
        This method also restores stripped '=' signs.

        Example:
        
            >>> enc = base64.b64encode('1')

            >>> enc
            >>> 'MQ=='

            >>> base64.b64decode(enc)
            >>> '1'

            >>> enc = enc.rstrip('=')

            >>> enc
            >>> 'MQ'

            >>> base64.b64decode(enc)
            ...
            TypeError: Incorrect padding

            >>> base64.b64decode(enc + '=' * (-len(enc) % 4))
            >>> '1'

        Args:
            value: The value to decode

        Returns:
            Base64-decoded string
        """
        return base64.b64decode(value + b'=' * (-len(value.strip()) % 4))