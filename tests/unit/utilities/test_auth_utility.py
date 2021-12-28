import pytest

from server.core.constants import Constants
from tests.test_constants import TestConstants


@pytest.mark.utility
def test_authenticate_password_authenticates_correct_password(auth_utility):
    password = TestConstants.PASSWORD
    authentic_password = auth_utility.encrypt_password(TestConstants.PASSWORD)

    expected = True
    actual = auth_utility.authenticate_password(
        password,
        authentic_password,
    )

    assert expected == actual


@pytest.mark.utility
def test_authenticate_password_fails_to_authenticate_incorrect_password(auth_utility):
    password = TestConstants.PASSWORD_TOO_SHORT
    authentic_password = auth_utility.encrypt_password(TestConstants.PASSWORD)

    expected = False
    actual = auth_utility.authenticate_password(
        password,
        authentic_password,
    )

    assert expected == actual


@pytest.mark.utility
def test_encrypt_and_decrypt_parameters_returns_parameters(auth_utility):
    expected = {
        'email': TestConstants.EMAIL,
        'password': TestConstants.PASSWORD,
    }

    encrypted_parameters = auth_utility.encrypt_parameters(
        expected,
        TestConstants.ENCRYPTION_KEY,
    )

    actual = auth_utility.decrypt_parameters(
        encrypted_parameters,
        TestConstants.ENCRYPTION_KEY,
    )

    assert expected == actual