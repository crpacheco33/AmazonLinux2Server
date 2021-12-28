from pydantic.error_wrappers import ValidationError

import pytest

from server.resources.schema.user import UserPasswordUpdateSchema
from tests.test_constants import TestConstants


def test_password_errors_when_password_is_short():
    with pytest.raises(ValidationError) as e:
        user = UserPasswordUpdateSchema(
            password=TestConstants.PASSWORD,
            new_password=TestConstants.PASSWORD_TOO_SHORT,
        )

    assert e.value.errors() == [
        {
            'loc': ('new_password',),
            'msg': 'Passwords should be between 8 and 32 characters long.',
            'type': 'value_error',
        }
    ]


def test_password_errors_when_password_is_missing_digit():
    with pytest.raises(ValidationError) as e:
        user = UserPasswordUpdateSchema(
            password=TestConstants.PASSWORD,
            new_password=TestConstants.PASSWORD_MISSING_DIGIT,
        )

    assert e.value.errors() == [
        {
            'loc': ('new_password',),
            'msg': 'Passwords must contain letters from the alphabet, at least one digit, uppercase and lowercase letter, and a symbol []()-+!.-="<>@~.',
            'type': 'value_error',
        }
    ]


def test_password_errors_when_password_is_missing_uppercase_letter():
    with pytest.raises(ValidationError) as e:
        user = UserPasswordUpdateSchema(
            password=TestConstants.PASSWORD,
            new_password=TestConstants.PASSWORD_MISSING_UPPERCASE_LETTER,
        )

    assert e.value.errors() == [
        {
            'loc': ('new_password',),
            'msg': 'Passwords must contain letters from the alphabet, at least one digit, uppercase and lowercase letter, and a symbol []()-+!.-="<>@~.',
            'type': 'value_error',
        }
    ]


def test_password_errors_when_password_is_missing_symbol():
    with pytest.raises(ValidationError) as e:
        user = UserPasswordUpdateSchema(
            password=TestConstants.PASSWORD,
            new_password=TestConstants.PASSWORD_MISSING_SYMBOL,
        )

    assert e.value.errors() == [
        {
            'loc': ('new_password',),
            'msg': 'Passwords must contain letters from the alphabet, at least one digit, uppercase and lowercase letter, and a symbol []()-+!.-="<>@~.',
            'type': 'value_error',
        }
    ]


def test_password_errors_when_password_is_missing_lowercase_letter():
    with pytest.raises(ValidationError) as e:
        user = UserPasswordUpdateSchema(
            password=TestConstants.PASSWORD,
            new_password=TestConstants.PASSWORD_MISSING_LOWERCASE_LETTER,
        )

    assert e.value.errors() == [
        {
            'loc': ('new_password',),
            'msg': 'Passwords must contain letters from the alphabet, at least one digit, uppercase and lowercase letter, and a symbol []()-+!.-="<>@~.',
            'type': 'value_error',
        }
    ]


def test_password_passes_when_password_is_valid():
    try:
        user = UserPasswordUpdateSchema(
            password=TestConstants.PASSWORD,
            new_password=TestConstants.PASSWORD,
        )
    except ValidationError as e:
        pytest.fail(e)
