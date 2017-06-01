'''
Chado Helper Module
'''
import psycopg2 as psql
import getpass
import types            # used to copy has_.. functions
import os               # environ -> db pw
from utility import PostgreSQLQueries as PSQLQ
from utility import make_namedtuple_with_query
from utility import Task
from StringIO import StringIO
from itertools import izip_longest as zip_pad

DB='drupal7'
USER='drupal7'
HOST='127.0.0.1'
PORT=5432

CULTIVAR_WIKI='''\
Germplasm Type: assemblage of plants select for desirable characteristics\
'''

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
                                    'nd_geolocation', 'dbxref', 'stockprop']
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
            for i in range(1, 4):
                if self.con:
                    break
                try: 
                    pw = getpass.getpass(prompt=prompt.format(n=i, db=db,
                        usr=usr, host=host, port=port))
                    self.con = psql.connect(database=db, user=usr, password=pw,
                        host=host, port=port)
                except psql.OperationalError as e:
                    pass
        if not self.con:
            raise e

        self.c = self.con.cursor()

    def __exe(self, query):
        '''execute + fetchall, remembers last query and result'''
        self.c.execute(query)
        self.lastq = query
        self.last_res = self.c.fetchall()
        return self.last_res

    def __tab_contains(self, name, table='', column='name'):
        '''Return True if Chado's '{table}' table contains an entry with
        <column> = <name>.'''
        if not table:
            raise RuntimeError('need \'table\' argument')
        sql = PSQLQ.select_all_from_where_eq
        sql = sql.format(table=table, name=name, col=column)
        if self.__exe(sql):
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

    @staticmethod
    def insert_into(table, values, columns=None, cursor=None):
        '''INSERT INTO <table> (columns) VALUES (values)
        
        Needs a cursor to execute the insert statement.
        '''
        if not type(values[0]) in [list, tuple]:
            msg = 'expected values = [[...], [...], ...]\n\tbut received: {}'
            msg = msg.format(str(values)[:50])
            raise RuntimeError(msg)
        if not cursor:
            raise RuntimeError('need cursor object')
        sql = PSQLQ.insert_into_table
        f = StringIO('\n'.join('\t'.join(str(v) for v in vs) for vs in values))
        #columns = '('+','.join(columns)+')'
        cursor.copy_from(f, table, columns=columns)

    @staticmethod
    def fetch_y_insert_into(fetch_stmt, values_constructor, *insert_args,
                            **insert_kwargs):
        '''Execute a query, join the values passed to the insert_into function
        with the fetch()ed result.
        
        Note: We could do this as a single sql statement, but we have a lot of
        data to handle, and we don't want to construct incredebly long strings.
        '''
        if not insert_kwargs.has_key('cursor'):
            raise RuntimeError('need cursor object')
        c = insert_kwargs['cursor']
        c.execute(fetch_stmt)
        fetch_res = c.fetchall()
        # replaces values with the constructed join'ed ones
        args = list(insert_args)
        args[1] = values_constructor(insert_args[1], fetch_res)
        ChadoPostgres.insert_into(*args, **insert_kwargs)

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

    def count_from(self, table):
        '''Return row count from the given table.'''
        r = self.__exe(PSQLQ.select_count.format(table=table))[0][0]
        self.last_res = r
        return r
   
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

    def __delete_where(self, table, condition):
        '''DELETE FROM <table> WHERE <condition>'''
        sql = PSQLQ.delete_where
        sql = sql.format(table=table, cond=condition)
        self.c.execute(sql)
        if not self.con.autocommit:
            self.con.commit()

    def delete_organism(self, genus, species):
        '''Deletes all organisms, with given genus and species.'''
        cond = '''genus = '{0}' and species = '{1}' '''
        cond = cond.format(genus, species)
        self.__delete_where('organism', cond)

    def delete_cvterm(self, name, cv, and_dbxref=False):
        '''Deletes all cvterms, with given name and cv.'''
        cvs = self.get_cv(where="name = '{}'".format(cv))
        if not len(cvs) == 1:
            raise Warning('Ambiguous cvterm deletetion.')
        cv_id = cvs[0].cv_id
        cond = "name = '{0}' and cv_id = {1}".format(name, cv_id)
        if and_dbxref:
            cvts = self.get_cvterm(where=cond)
        self.__delete_where('cvterm', cond)
        if and_dbxref:
            cond = "accession = '{}'".format(name)
            self.__delete_where('dbxref', cond)

    def delete_stock(self, stock):
        cond = "uniquename = '{}'".format(stock)
        self.__delete_where('stock', cond)

    def delete_geolocation(self, site='', keys={}):
        '''<keys> are concatenated and "AND"-ed as =equal= conditions to the
        delete condition, site ist checked against 'description'.
        '''
        if site:
            cond = "description = '{}'".format(site)
        else: 
            cond = '1=1'
        if keys:
            ndg = 'nd_geolocation.'
            for k,v in keys.iteritems():
                if k.startswith(ndg): k = k.split(ndg)[1]
                cond = cond + " and {col} = '{val}'".format(col=k, val=v)
        self.__delete_where('nd_geolocation', cond)

    def delete_phenotype(self, pheno):
        pass

class ChadoDataLinker(object):
    '''Links large list()s of data, ready for upload into Chado.
    
    The create_* functions return Task-objects, that when execute()d, will link
    and upload the given data into the chado schema.
    '''

    # Note: order matters, as we zip() dbxrefs at the end v 
    CVTERM_COLS = ['cv_id', 'definition', 'name', 'dbxref_id']
    DBXREF_COLS = ['db_id', 'accession']
    STOCK_COLS  = ['organism_id', 'name', 'uniquename', 'type_id']
    GEOLOC_COLS = ['description', 'latitude', 'longitude', 'altitude']
    PHENO_COLS  = ['uniquename', 'attr_id', 'value']

    def __init__(self, chado, dbname, cvname):
        self.db = dbname
        self.cv = cvname
        self.chado = chado
        self.con = chado.con
        self.c = self.con.cursor()

    def create_cvterm(self, cvterm, accession=[], definition=[], tname=None):
        '''Create (possibly multiple) Tasks to upload cvterms.'''
        if not accession:
            accession = cvterm
        if len(cvterm) != len(accession) or \
                definition and len(cvterm) != len(definition):
            raise RuntimeError('argument length unequal!')

        cv_id = self.chado.get_cv(where="name = '{}'".format(self.cv))[0].cv_id
        db_id = self.chado.get_db(where="name = '{}'".format(self.db))[0].db_id
        all_dbxrefs = self.chado.get_dbxref(where="db_id = '{}'".format(db_id))
        curr_dbxrefs = [d.accession for d in all_dbxrefs]
        needed_dbxrefs = [a for a in accession if not a in curr_dbxrefs]

        content = [[db_id, a] for a in needed_dbxrefs]
        name = 'dbxref upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('dbxref', content, self.DBXREF_COLS)
        kwargs = {'cursor' : self.con.cursor()} # new cursor() for every Task
        t1_dbxref = Task(name, f, *args, **kwargs)

        # Note: Scary code. We don't know dbxref_id's but still want to use
        # the copy_from upload, or at least an execute query that uploads all
        # our data at once, and not upload 1 dbxref, 1 cvterm at a time.

        # cvterm names will be added later to <content> in order to ensure
        # correct ordering, because we don't know the dbxref_id yet
        if definition:
            it = zip(cvterm, definition)
            content = [[cv_id, de] for c,de in it] 
        else:
            content = [[cv_id, ''] for c in cvterm]
        name = 'cvterm upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into # XXX add fetch..
        fstmt = '''\
            SELECT accession,dbxref_id
                FROM dbxref JOIN (VALUES {}) v ON v.column1 = dbxref.accession
        '''
        fstmt = fstmt.format(','.join("('{}')".format(v) for v in cvterm))
        def join_func(x, y):
            return [[i[0],i[1],j[0],j[1]] for i,j in zip(x,y)]
        args = (fstmt, join_func, 'cvterm', content, self.CVTERM_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        t2_cvt = Task(name, f, *args, **kwargs)
        return (t1_dbxref, t2_cvt) # tuple = enforce sequencial execution

    def create_stock(self, stocks, organism, tname=None, germplasm_t='cultivar'):
        '''Create (possibly multiple) Tasks to upload stocks.'''
        where = 'name = \'{}\''.format(germplasm_t)
        cvt = self.chado.get_cvterm(where=where)[0]
        if not cvt:
            for t in self.create_cvterm([germplasm_t],
                                        definition=[CULTIVAR_WIKI]):
                t.execute()
            cvt = self.chado.get_cvterm(where=where)[0]
        type_id = cvt.cvterm_id
        o_id = organism.organism_id
        content = [[o_id, s, s, type_id] for s in stocks]
        args = ('stock', content, self.STOCK_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        f = self.chado.insert_into
        name = 'stock upload'
        if tname: name = name + '({})'.format(tname)
        t = Task(name, f, *args, **kwargs)
        return (t,)

    def create_geolocation(self, sites, tname=None):
        '''Create (possibly multiple) Tasks to upload geolocations.'''
        # Our translator creates this funny format, deal with it.
        content = [[i['nd_geolocation.description'],
                    i['nd_geolocation.latitude'],
                    i['nd_geolocation.longitude'], i['nd_geolocation.altitude']]
                    for i in sites]
        args = ('nd_geolocation', content, self.GEOLOC_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        f = self.chado.insert_into
        name = 'geolocation upload'
        if tname: name = name + '({})'.format(tname)
        t = Task(name, f, *args, **kwargs)
        return (t,)

    def create_stockprop(self, props, tname=None):
        #content = [[s,t,v] for s,t,v in zip(stock_ids, type_ids, values)]
        return [Task('Empty', lambda a,b: None, [], {})]

    def create_phenotype(self, phenos, tname=None):
        name = 'phenotype upload'
        #content = [[n, a, v] for n,a,v in zip(uniqs, attrs, vals)]
        args = ('phenotype', content, self.PHENO_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        if tname: name = name + '({})'.format(tname)
        t = Task(name, f, *args, **kwargs)
        return [Task('Empty', lambda a,b: None, [], {})]

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
for table in ChadoPostgres.COMMON_TABLES + ['organism', 'nd_geolocation',
        'dbxref', 'stockprop']:
    prefix = 'get_'
    newfget_name = prefix+table
    if not hasattr(ChadoPostgres, newfget_name):
        newf = copy_func(ChadoPostgres._ChadoPostgres__get_rows, newfget_name)
        newf.func_defaults = (table,'',False)
        newf.func_doc = ChadoPostgres._ChadoPostgres__get_rows\
                                     .__doc__.format(table=table)
        setattr(ChadoPostgres, prefix+table, newf)
# META-END
