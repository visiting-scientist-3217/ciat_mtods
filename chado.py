#!/usr/bin/python
'''Chado Helper Module'''

import psycopg2 as psql
import getpass
import types # metaprogramming

def copy_func(f, newname):
    return types.FunctionType(
        f.func_code, f.func_globals, newname or f.func_name, f.func_defaults,
        f.func_closure
    )

DB='drupal7'
USER='postgres'

class ChadoPostgres():
    '''You can ask questions about Chado once we have a connection to that
    database.'''
    # TODO: Get config from a file/cmd-line.

    # Note: These strings are used for method creation of the form:
    #       .has_db .has_cv ...
    COMMON_TABLES = ['db', 'cv', 'cvterm', 'genotype', 'phenotype', 'project',
                     'stock', 'study']

    def __init__(self, db=DB, usr=USER):
        # Get db connection.
        self.con = None
        try:
            self.con = psql.connect(database=db, user=usr)
        except psql.OperationalError:
            prompt = '{n}/3 Password for psql connection to {db} as {usr}: '
            i = 1 # 3 tries
            while i < 4 and not self.con:
                try: 
                    pw = getpass.getpass(prompt=prompt.format(n=i, db=db,
                                         usr=usr))
                    self.con = psql.connect(database=DB, user=USER, password=pw)
                except psql.OperationalError as e:
                    pass
                finally:
                    i += 1
        if not self.con:
            raise e #RuntimeError('Could not connect to the PGSQL')

        self.c = self.con.cursor()

    def tab_contains(self, name, table=''):
        '''Return True if Chado's '{table}' table contains an entry with name =
        <name>.'''
        if not table:
            raise RuntimeError('need \'table\' argument')
        sql = '''select * from {table} where name = '{name}' '''
        sql = sql.format(table=table, name=name)
        self.c.execute(sql)
        self.lastq = self.c.fetchall()
        if self.lastq:
            return True
        else:
            return False

    def exe(self, query):
        '''self.c.execute + fetchall'''
        try:
            self.c.execute(query)
            self.lastq = self.c.fetchall()
            return True
        except Exception as e:
            return e

# Create convenient methods:
#   .has_db() .has_cv() .has_cvterm() ...
for table in ChadoPostgres.COMMON_TABLES:
    newf_name = 'has_'+table
    if not hasattr(ChadoPostgres, newf_name):
        newf = copy_func(ChadoPostgres.tab_contains, newf_name)
        newf.func_defaults = (table,)
        newf.func_doc = ChadoPostgres.tab_contains.__doc__.format(table=table)
        setattr(ChadoPostgres, 'has_'+table, newf)
