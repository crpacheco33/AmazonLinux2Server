import csv
import datetime
import json
import logging
import os
import sys
import traceback

from bson.objectid import ObjectId
from pydantic import ValidationError

import click
import pymongo
import requests

from server.core.constants import Constants
from server.resources.schema.onboard import UserOnboardSchema
from server.resources.schema.user import (
    PasswordWillResetSchema,
    PasswordWillResetSchema,
    ResendEmailSchema,
    User2FASchema,
    UserInvitationSchema,
    UserLoginSchema,
)
from server.services.auth_service import AuthService
from server.services.aws_service import AWSService
from server.services.twilio_service import TwilioService
from server.services.user_service import UserService


log = AWSService().log_service
log.handler = logging.StreamHandler(
    sys.stdout,
)


class ClickStringOptionToList(click.Option):

    def type_cast_value(self, ctx, value):
        if value == '': return
        try:
            value = str(value)
            assert value.count('[') == 1 and value.count(']') == 1
            list_as_str = value.replace('"', "'").split('[')[1].split(']')[0]
            list_of_items = [item.strip().strip("'") for item in list_as_str.split(',')]
            return list_of_items
        except Exception:
            raise click.BadParameter(value)


class UserContext:

    def __init__(self):
        pass


@click.group()
@click.pass_context
def users(ctx):
    ctx.obj = UserContext()


@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('email', required=False)
@click.argument('name', required=False)
@click.option('--scopes', '-s', default=[], required=False, cls=ClickStringOptionToList, type=click.Choice(['read', 'write', 'admin']))
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
@click.pass_obj
def add(obj, email, name, scopes, path):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    user_service = UserService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    with client.start_session() as session:
        collection = client.visibly.users

        indices = collection.index_information()
        if indices.get('email') is None:
            collection.create_index(
                [( 'email', pymongo.DESCENDING, )],
                unique=True,
            )

    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            email = user.get('email')
            name = user.get('name')
            scopes = user.get('scopes')
            
            try:
                user_service.add(email, name, scopes, client)
            except Exception as e:
                log.error(f'User {name} was not invited')
                traceback.print_exc(e)
    else:
        try:
            user_service.add(email, name, scopes, client)
        except Exception as e:
            log.error(f'User {name} was not invited')
            traceback.print_exc(e)


@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
@click.pass_obj
def add_brands(obj, path):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    user_service = UserService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client

    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            user_id = user.get('id')
            brand_ids = user.get('brands', [])
            
            try:
                user_service.add_brands(user_id, brand_ids, client)
            except Exception as e:
                log.error(f'Brands were not added to user {user_id}')
                traceback.print_exc(e)

@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('email')
@click.argument('name')
@click.option('--brands', '-b', default=[], required=False, cls=ClickStringOptionToList)
@click.option('--scopes', '-s', default=[], required=False, cls=ClickStringOptionToList)
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
@click.pass_obj
def e2e(obj, email, name, brands, scopes, path):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    auth_service = AuthService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            email = user.get('email')
            name = user.get('name')
            password = user.get('password')
            scopes = user.get('scopes')
            brands = user.get('brands')

            try:
                url = auth_service.invite(email, name, scopes, brands, client)
                encrypted_parameters = url.split('=')[1].split('&')[0]

                request = UserInvitationSchema(
                    hash=encrypted_parameters,
                    password=password,
                )
                auth_service.register(request, client)
                
                request = UserLoginSchema(
                    email=email,
                    password=password,
                )
                
                sid = auth_service.sign_in(request, client)

                log.info(f'\nUser {email} received an email to sign in using password {password}\n')
            except Exception as e:
                log.error(f'User {name} was not E2E signed in')
                traceback.print_exc(e)
    else:
        if not _validate_scopes(scopes):
            print('Scopes can only use "read", "write", or "admin" values')
            exit(1)
        
        password = 'password'
        try:
            url = auth_service.invite(email, name, scopes, brands, client)
            encrypted_parameters = url.split('=')[1].split('&')[0]

            request = UserInvitationSchema(
                hash=encrypted_parameters,
                password=password,
            )
            auth_service.register(request, client)
            
            request = UserLoginSchema(
                email=email,
                password=password,
            )
            
            sid = auth_service.sign_in(request, client)
            print(
                f'\nCheck {email}\'s Inbox and copy the link from Visibly\'s email to this command:',
            )
            print(
                f'\n> curl URL_FROM_EMAIL\n',
            )
            print(f'...and copy the value of the `code=CODE` in the response output to this prompt.\n')
            code = input('Please input the verification code: ')
            
            credentials = User2FASchema(
                code=code,
                sid=sid,
            )
            access_token, expires_in, refresh_token = auth_service.authenticate(
                credentials,
                client,
            )
            log.info(f'\nUser {email} is signed in with access token {access_token}\n')
        except Exception as e:
            log.error(f'User {name} was not E2E signed in')
            traceback.print_exc(e)


@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('email', required=False)
@click.option('--brands', '-b', default=[], required=False, cls=ClickStringOptionToList)
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
@click.pass_obj
def invite(obj, email, brands, path):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    user_service = UserService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            email = user.get('email')
            brands = user.get('brands')
            
            try:
                user_service.invite(email, brands, client)
            except Exception as e:
                log.error(f'User {email} was not invited')
                traceback.print_exc(e)
    else:
        try:
            user_service.invite(email, brands, client)
        except Exception as e:
            log.error(f'User {email} was not invited')
            traceback.print_exc(e)
                
                
@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=True, type=click.Path(exists=True))
@click.pass_obj
def onboard(obj, path):
    users = []
    with open(path) as read_file:
        rows = csv.DictReader(
            read_file,
            fieldnames=["ignore_1", "email", "ignore_2", "brands_na", "brands_apac", "brands_emea", "email_confirmation", "name", "email_manager"],
        )

        next(rows)  # Skip headers

        for row in rows:
            brands = []
            brands_na = row.get('brands_na', '')
            brands_na = brands_na.split(Constants.COMMA)
            brands_na = [brand.strip() for brand in brands_na]
            brands.extend(brands_na)
            
            brands_apac = row.get('brands_apac', '')
            brands_apac = brands_apac.split(Constants.COMMA)
            brands_apac = [brand.strip() for brand in brands_apac]
            brands.extend(brands_apac)
            
            brands_emea = row.get('brands_emea', '')
            brands_emea = brands_emea.split(Constants.COMMA)
            brands_emea = [brand.strip() for brand in brands_emea]
            brands.extend(brands_emea)

            brands = [brand for brand in brands if brand not in [None, Constants.EMPTY_STRING]]
            
            user = {
                'email': row.get('email'),
                'name': row.get('name'),
                'brands': brands,
                'scopes': [
                    Constants.SCOPE_READ,
                    Constants.SCOPE_WRITE,
                ]
            }

            try:
                UserOnboardSchema(**user)
                users.append(user)
            except ValidationError as e:
                raise e
                exit(1)
    
    with open(f'users_{datetime.datetime.utcnow().date()}.json', 'wt') as write_file:
        write_file.write(json.dumps(users))

    
@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=True, type=click.Path(exists=True))
@click.pass_obj
def remove(obj, path):
    aws_service = AWSService()
    client = aws_service.docdb_service.client
    
    with open(path) as read_file:
        users = json.loads(read_file.read())

    for user in users:
        user_id = user.get('id')

        log.info(
            f'Removing user {user_id} from brands...',
        )
        with client.start_session() as session:
            collection = client.visibly.brands
            collection.update_many(
                {},
                { '$pull': { 'users': ObjectId(user_id) } },
            )
        log.info(
            f'Removed user {user_id} from brands',
        )
        
        log.info(
            f'Removing user {user_id} from users...',
        )
        with client.start_session() as session:
            collection = client.visibly.users
            collection.remove(
                { '_id': ObjectId(user_id) },
            )
        log.info(
            f'Removed user {user_id} from users',
        )

    
@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.option('--path', '-p', required=False, type=click.Path(exists=True))
@click.pass_obj
def remove_brands(obj, path):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    user_service = UserService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client

    if path:
        with open(path) as read_file:
            users = json.loads(read_file.read())

        for user in users:
            user_id = user.get('id')
            brands = user.get('brands', [])
            
            try:
                user_service.remove_brands(user_id, brand_ids, client)
            except Exception as e:
                log.error(f'Brands were not removed from user {user_id}')
                traceback.print_exc(e)

@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('invitation')
@click.argument('password')
@click.pass_obj
def register(obj, invitation, password):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    auth_service = AuthService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    request = UserInvitationSchema(
        hash=invitation,
        password=password,
    )
    try:
        auth_service.register(request, client)
    except Exception as e:
        log.error(f'User was not registered')
        traceback.print_exc(e)


@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('email')
@click.argument('type')
@click.pass_obj
def resend_email(obj, email, type):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    auth_service = AuthService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    request = ResendEmailSchema(
        email=email,
        email_type=type,
    )

    try:
        auth_service.will_resend_email(
            request,
            client,
        )
    except Exception as e:
        log.error(f'User {email} was not able to resend email')
        traceback.print_exc(e)

@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('email')
@click.pass_obj
def reset_password(obj, email):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    auth_service = AuthService(
        ssm_service,
        TwilioService(),
    )
    password = 'new_password'
    client = aws_service.docdb_service.client
    
    request = PasswordWantsResetSchema(
        email=email,
    )
    sid = auth_service.wants_reset_password(request, client)
    
    print(
        f'\nCheck {email}\'s Inbox and copy the link from Visibly\'s email to this command:',
    )
    print(
        f'\n> curl URL_FROM_EMAIL\n',
    )
    print(f'...and copy the value of the `code=CODE` in the response output to this prompt.\n')
    code = input('Please input the verification code: ')

    request = PasswordWillResetSchema(
        code=code,
        email=email,
        password=password,
        sid=sid,
    )
    auth_service.will_reset_password(request, client)
    

@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('email')
@click.argument('password')
@click.pass_obj
def sign_in(obj, email, password):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    auth_service = AuthService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    request = UserLoginSchema(
        email=email,
        password=password,
    )
    try:
        sid = auth_service.sign_in(request, client)
        print(
            f'\nCheck {email}\'s Inbox and copy the link from Visibly\'s email to this command:',
        )
        print(
            f'\n> curl URL_FROM_EMAIL\n',
        )
        print(f'...and copy the value of the `code=CODE` in the response output to this prompt.\n')
        code = input('Please input the verification code: ')
        
        credentials = User2FASchema(
            code=code,
            sid=sid,
        )
        access_token, _, _ = auth_service.authenticate(
            credentials,
            client,
        )
        log.info(f'\nUser {email} is signed in with access token {access_token}\n')
    except Exception as e:
        log.error(f'User {email} was not signed in')
        traceback.print_exc(e)


@users.command(
    context_settings={
        'allow_extra_args': True,
        'ignore_unknown_options': True,
    },
)
@click.argument('code')
@click.argument('sid')
@click.pass_obj
def tfa(obj, code, sid):
    aws_service = AWSService()
    ssm_service = aws_service.ssm_service
    auth_service = AuthService(
        ssm_service,
        TwilioService(),
    )
    client = aws_service.docdb_service.client
    
    request = User2FASchema(
        code=code,
        sid=sid,
    )
    try:
        access_token, expires_in, refresh_token = auth_service.authenticate(
            request,
            client,
        )
    except Exception as e:
        log.error(f'User 2FA was not confirmed')
        traceback.print_exc(e)


def _validate_scopes(scopes):
    for scope in scopes:
        if scope not in [
            Constants.SCOPE_ADMIN,
            Constants.SCOPE_READ,
            Constants.SCOPE_WRITE,
        ]:
            return False
    
    return True