#!/usr/bin/python
'''Chado Helper Module'''

import psycopg2 as psql
import getpass
import types            # used to copy has_.. functions
import os               # environ -> db pw
from utility import PostgreSQLQueries as PSQLQ
from utility import make_namedtuple_with_query

DB='drupal7'
USER='drupal7'
HOST='127.0.0.1'
PORT=5432

class ChadoPostgres():
    '''You can ask questions about Chado once we have a connection to that
    database.
    
    Note: Initialization includes connection establishment to the database.
    Note: We create some methods dynamically.

    All 'has_'-functions return True if <table> contains sth. called <name>.
    Namely:
        .has_{table}(name)
            , with {table} element [self.COMMON_TABLES, 'organism',
                                    'nd_geolocation']

    All 'get_'-functions Return all rows from {table} as namedtuple, with
    <where> used as where statement. If <where> is empty all rows are returned.
        .get_{table}(where='')
            , with {table} element [self.COMMON_TABLES, 'organism',
                                    'nd_geolocation']
    '''
    COMMON_TABLES = ['db', 'cv', 'cvterm', 'genotype', 'phenotype', 'project',
                     'stock', 'study']

    def __init__(self, db=DB, usr=USER, host=HOST, port=PORT):
        '''Without host and port, we default to localhost:5432.'''
        self.__connect(db, usr, host, port)

    def __connect(self, db, usr, host, port):
        '''Establish the connection.'''
        self.con = None
        pw = None
        if os.environ.has_key('POSTGRES_PW'):
            pw = os.environ['POSTGRES_PW']
        try:
            self.con = psql.connect(database=db, user=usr, host=host,
                port=port, password=pw)
        except psql.OperationalError as e:
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

    def __exe(self, query):
        '''self.c.execute + self.lastq = fetchall()'''
        self.c.execute(query)
        self.lastq = self.c.fetchall()
        return self.lastq

    def __tab_contains(self, name, table='', column='name'):
        '''Return True if Chado's '{table}' table contains an entry with
        <column> = <name>.'''
        if not table:
            raise RuntimeError('need \'table\' argument')
        sql = PSQLQ.select_all_from_where_eq
        sql = sql.format(table=table, name=name, col=column)
        self.__exe(sql)

        if self.lastq:
            return True
        else:
            return False

    def __get_rows(self, table='', where='', rawdata=False):
        '''Return all rows from {table} as namedtuple
        
        <where> is used as where statement. If where is empty all rows are
        returned.
        If <rawdata> is True, we return the fetched rows as list().
        '''
        sql = PSQLQ.select_all
        if where:
            sql = sql + ' WHERE {where}'
            sql = sql.format(table=table, where=where)
        else:
            sql = sql.format(table=table)

        raw_result = self.__exe(sql)
        if rawdata:
            return raw_result

        sql = PSQLQ.column_names.format(table=table)
        result = make_namedtuple_with_query(self.c, sql, table, raw_result)
        return result

    # Following function definitions where made manually, as the column name
    # differs from the standart name 'name'.
    def has_genus(self, name):
        '''See __tab_contains'''
        return self.__tab_contains(name, table='organism', column='genus')

    def has_species(self, name):
        '''See __tab_contains'''
        return self.__tab_contains(name, table='organism', column='species')

    def has_nd_geolocation(self, name):
        '''See __tab_contains'''
        return self.__tab_contains(name, table='nd_geolocation', column='description')

    #def get_organism(self, genus='', species=''):
    #    self.__exe(PSQLQ.select_all

   
    def create_organism(self, genus, species, abbreviation='', common_name='',
        comment=''):
        '''Create an organism with the given specifications.'''
        sql = PSQLQ.insert_into_table
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
        sql = PSQLQ.delete_where
        condition = '''genus = '{genus}' and species = '{species}' '''
        condition = condition.format(genus=genus, species=species)
        sql = sql.format(table='organism', cond=condition)

        self.c.execute(sql)
        if not self.con.autocommit:
            self.con.commit()

# META-BEGIN
# Metaprogramming helper-function.
def copy_func(f, newname):
    return types.FunctionType(
        f.func_code, f.func_globals, newname or f.func_name, f.func_defaults,
        f.func_closure
    )

# Create convenient methods:
#   .has_db() .has_cv() .has_cvterm() ...
for table in ChadoPostgres.COMMON_TABLES:
    prefix = 'has_'
    newf_name = prefix+table
    if not hasattr(ChadoPostgres, newf_name):
        newf = copy_func(ChadoPostgres._ChadoPostgres__tab_contains, newf_name)
        newf.func_defaults = (table,)
        newf.func_doc = ChadoPostgres._ChadoPostgres__tab_contains\
                                     .__doc__.format(table=table)
        setattr(ChadoPostgres, prefix+table, newf)

# Create convenient methods:
#   .get_db() .get_cv() .get_..
for table in ChadoPostgres.COMMON_TABLES + ['organism', 'nd_geolocation']:
    prefix = 'get_'
    newfget_name = prefix+table
    if not hasattr(ChadoPostgres, newfget_name):
        newf = copy_func(ChadoPostgres._ChadoPostgres__get_rows, newfget_name)
        newf.func_defaults = (table,'',False)
        newf.func_doc = ChadoPostgres._ChadoPostgres__get_rows\
                                     .__doc__.format(table=table)
        setattr(ChadoPostgres, prefix+table, newf)
# META-END
