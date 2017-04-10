#!/usr/bin/python
'''Chado Helper Module'''

import psycopg2 as psql
import getpass
import types # metaprogramming
import os # environ -> db pw

def copy_func(f, newname):
    return types.FunctionType(
        f.func_code, f.func_globals, newname or f.func_name, f.func_defaults,
        f.func_closure
    )

DB='drupal7'
USER='postgres'
HOST='localhost'
PORT=5432

class ChadoPostgres():
    '''You can ask questions about Chado once we have a connection to that
    database.
    
    Note: Initialization is equal to connection to the database.
    Note: We create some methods dynamically, namely:
        .has_{table}(name)
            , with {table} e{ self.COMMON_TABLES }
    '''
    COMMON_TABLES = ['db', 'cv', 'cvterm', 'genotype', 'phenotype', 'project',
                     'stock', 'study']

    def __init__(self, db=DB, usr=USER, host=HOST, port=PORT):
        '''Without host and port, we default to localhost:5432.'''
        # Get db connection.
        self.con = None
        pw = None
        if os.environ.has_key('POSTGRES_PW'):
            pw = os.environ['POSTGRES_PW']
        try:
            self.con = psql.connect(database=db, user=usr, host=host,
                port=port, password=pw)
        except psql.OperationalError:
            prompt = '{n}/3 psql connection to {db} as {usr} @'\
                    +' {host}:{port}\nPassword: '
            i = 1 # 3 tries
            while i < 4 and not self.con:
                try: 
                    pw = getpass.getpass(prompt=prompt.format(n=i, db=db,
                        usr=usr, host=host, port=port))
                    self.con = psql.connect(database=db, user=usr, password=pw,
                        host=host, port=port)
                except psql.OperationalError as e:
                    pass
                finally:
                    i += 1
        if not self.con:
            raise e #RuntimeError('Could not connect to the PGSQL')

        self.c = self.con.cursor()

    def tab_contains(self, name, table='', column='name'):
        '''Return True if Chado's '{table}' table contains an entry with
        <column> = <name>.'''
        if not table:
            raise RuntimeError('need \'table\' argument')
        sql = PostgreSQLQueries.select_all_from_where_eq
        sql = sql.format(table=table, name=name, col=column)
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

    def has_genus(self, name):
        '''See tab_contains'''
        return self.tab_contains(name, table='organism', column='genus')

    def has_species(self, name):
        '''See tab_contains'''
        return self.tab_contains(name, table='organism', column='species')

   
    def create_organism(self, genus, species, abbreviation='', common_name='',
        comment=''):
        '''Create an organism with the given specifications.'''
        sql = PostgreSQLQueries.insert_into_table
        columns = '(abbreviation, genus, species, common_name, comment)'
        table = 'organism'
        values = '''('{abr}', '{gen}', '{spec}', '{com_n}', '{com}')'''
        values = values.format(abr=abbreviation, gen=genus, spec=species,
            com_n=common_name, com=comment)
        sql = sql.format(table=table, columns=columns, values=values)

        self.c.execute(sql)
        if not self.con.autocommit:
            self.con.commit()

    def delete_organism(self, genus, species):
        '''Deletes all organisms, with given genus and species.'''
        sql = PostgreSQLQueries.delete_where
        condition = '''genus = '{genus}' and species = '{species}' '''
        condition = condition.format(genus=genus, species=species)
        sql = sql.format(table='organism', cond=condition)

        self.c.execute(sql)
        if not self.con.autocommit:
            self.con.commit()

class PostgreSQLQueries():
    select_all_from_where_eq = '''\
        SELECT * FROM {table} WHERE {col} = '{name}'\
    '''
    insert_into_table = '''\
        INSERT INTO {table} {columns} VALUES {values}\
    '''
    delete_where = '''\
        DELETE FROM {table} WHERE {cond}\
    '''

# Create convenient methods:
#   .has_db() .has_cv() .has_cvterm() ...
for table in ChadoPostgres.COMMON_TABLES:
    newf_name = 'has_'+table
    if not hasattr(ChadoPostgres, newf_name):
        newf = copy_func(ChadoPostgres.tab_contains, newf_name)
        newf.func_defaults = (table,)
        newf.func_doc = ChadoPostgres.tab_contains.__doc__.format(table=table)
        setattr(ChadoPostgres, 'has_'+table, newf)
