import base64
import secrets

from cryptography.fernet import Fernet


def encryption_key():
    """Used to encrypt passwords and parameters for Twilio."""
    key = Fernet.generate_key()
    key = base64.b64encode(key).decode('utf-8')

    return key


def application_secret():
    """Used to encrypt application secrets (access tokens, etc.)."""
    return secrets.token_urlsafe(64)


if __name__ == '__main__':
    print('Creating new encryption key...')
    encryption_key = encryption_key()
    print('Creating new application secret...')
    application_secret = application_secret()

    print(
        f'Your new encryption key is {encryption_key}',
    )
    print(
        f'Your new application secret is {application_secret}',
    )

    print(
        'Add these to your application\'s SSM Parameter Store'
    )