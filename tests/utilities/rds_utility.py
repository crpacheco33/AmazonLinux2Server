import string
import psycopg2


class RDSUtility:
    
    def __init__(self, engine, database='visibly', role='visibly', schema='visibly'):
        self._connection = None
        
        self._engine = engine
        self._database = database
        self._role = role
        self._schema = schema

        self._create_role_command = string.Template('CREATE ROLE ${role} LOGIN')
        self._create_schema_command = string.Template('CREATE SCHEMA IF NOT EXISTS ${schema}')
        self._drop_schema_command = string.Template('DROP SCHEMA IF EXISTS ${schema} CASCADE')
        self._grant_all_privileges_on_database_command = string.Template(
            'GRANT ALL PRIVILEGES ON DATABASE ${database} TO ${role}',
        )
        self._grant_all_privileges_on_tables_command = string.Template(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA ${schema} GRANT ALL PRIVILEGES ON TABLES TO ${role}',
        )
        self._grant_all_privileges_to_user_command = string.Template(
            'GRANT ALL PRIVILEGES ON SCHEMA ${schema} TO ${role}',
        )
        self._grant_rds_iam_to_user_command = string.Template(
            'GRANT rds_iam TO ${role}',
        )
        self._grant_rds_superuser_to_user_command = string.Template(
            'GRANT rds_superuser TO ${role}',
        )
        self._role_exists_command = string.Template('SELECT 1 FROM pg_roles WHERE pg_roles.rolname = \'${role}\'')

    def create_role(self, role):
        create_role_statement = self._create_role_command.substitute({
            'role': role,
        })
        grant_iam_statement = self._grant_rds_iam_to_user_command.substitute({
            'role': role,
        })
        grant_superuser_statement = self._grant_rds_superuser_to_user_command.substitute({
            'role': role,
        })
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(create_role_statement)
                # cursor.execute(grant_iam_statement)
                # cursor.execute(grant_superuser_statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
        
        self._close_connection()

    def create_schema(self, schema):
        statement = self._create_schema_command.substitute({
            'schema': schema,
        })
        with self.connection.cursor() as cursor:
            try:
                print(statement)
                cursor.execute(statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
        
        self._close_connection()

    def drop_schema(self, schema):
        statement = self._drop_schema_command.substitute({
            'schema': schema,
        })
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
        
        self._close_connection()

    def grant_all_privileges_on_database(self, role):
        statement = self._grant_all_privileges_on_database_command.substitute({
            'database': self._database,
            'role': role,
        })
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
        
        self._close_connection()

    def grant_all_privileges_on_tables(self, schema, role):
        statement = self._grant_all_privileges_on_tables_command.substitute({
            'schema': schema,
            'role': role,
        })
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
        
        self._close_connection()

    def grant_all_privileges_to_user(self, schema, role):
        statement = self._grant_all_privileges_to_user_command.substitute({
            'schema': schema,
            'role': role,
        })
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
        
        self._close_connection()

    def role_does_exist(self, role):
        role_does_exist = False
        statement = self._role_exists_command.substitute({
            'role': role,
        })
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(statement)
            except Exception as e:
                print(e)
            finally:
                self.connection.commit()
                role_does_exist = cursor.rowcount > 0
        
        self._close_connection()
        return role_does_exist

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self._engine.raw_connection()

        return self._connection

    def _close_connection(self):
        self.connection.close()
        self._connection = None
