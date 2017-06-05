'''
Chado Helper Module
'''
import psycopg2 as psql
import getpass
import types            # used to copy has_.. functions
import os               # environ -> db pw
import re
from utility import PostgreSQLQueries as PSQLQ
from utility import make_namedtuple_with_query
from utility import Task
from StringIO import StringIO
from itertools import izip_longest as zip_pad
from random import randint

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
        '''Execute a query, replace insert_args[1], which are the values,
        passed to the insert_into function with the result of the
        values_constructor function function, given the the original
        insert_args[1] and the result of the fetch_stmt statement as argument.
            args[1] = values_constructor(insert_args[1], fetch_result)
        
        <insert_kwargs> must contain a key called 'cursor' with a valid
        db-cursor as value.

        Note: We could do this as a single sql statement, but we have a lot of
        data to handle, and we don't want to construct incredebly long
        INSERT-statements. Like this we only construct moderately long
        SELECT-statements.
        '''
        print '[fetch_y_insert_into] args:', insert_args
        print '[fetch_y_insert_into] kwargs:', insert_kwargs
        if not insert_kwargs.has_key('cursor'):
            raise RuntimeError('need cursor object')
        c = insert_kwargs['cursor']
        c.execute(fetch_stmt)
        fetch_res = c.fetchall()
        # replaces values with the constructed join'ed ones
        args = list(insert_args)
        args[1] = values_constructor(insert_args[1], fetch_res)
        ChadoPostgres.insert_into(*args, **insert_kwargs)

    # Following function definitions where made manually, as the identifying
    # column name differs from 'name'
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
    CVTERM_COLS     = ['cv_id', 'definition', 'name', 'dbxref_id']
    DBXREF_COLS     = ['db_id', 'accession']
    STOCK_COLS      = ['organism_id', 'name', 'uniquename', 'type_id']
    STOCKPROP_COLS  = ['stock_id', 'type_id', 'value']
    GEOLOC_COLS     = ['description', 'latitude', 'longitude', 'altitude']
    PHENO_COLS      = ['uniquename', 'attr_id', 'value'] # not const
    EXP_COLS        = ['nd_geolocation_id', 'type_id']

    # implemented stockprop's which might occur in the phenotyping metadata
    STOCKPROPS = ('pick_date', 'plant_date')

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
        fstmt = '''\
            SELECT accession,dbxref_id
                FROM dbxref JOIN (VALUES {}) v ON v.column1 = dbxref.accession
        '''
        fstmt = fstmt.format(','.join("('{}')".format(v) for v in cvterm))
        def join_func(x, y):
            return [[i[0],i[1],j[0],j[1]] for i,j in zip(x,y)]

        name = 'cvterm upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into # XXX add fetch..
        insert_args = ['cvterm', content, self.CVTERM_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [fstmt, join_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        t2_cvt = Task(name, f, *args, **kwargs)
        return (t1_dbxref, t2_cvt) # tuple = enforce sequencial execution

    def __get_or_create_cvterm(self, term):
        '''Returns the cvterm named <term>, if it does not exist, we create it
        first.
        '''
        ts = self.create_cvterm([term], accession=[acs], tname='__get_or_create')
        for t in ts: t.execute()
        cvterm = self.chado.get_cvterm(where='name = {}'.format(term))[0]
        if not cvterm: raise RuntimeError('cvterm creation failed!')
        return cvterm

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

        name = 'stock upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('stock', content, self.STOCK_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        t = Task(name, f, *args, **kwargs)
        return (t,)

    def __check_coords(self, ignore_me, lat, lon, alt):
        ''''''
        r = []
        for i in [lat, lon, alt]:
            try: 
                i = re.sub(r'^\s*0*', '', i)
                i = re.sub(r'([0-9]*)[NE]', '+\\1', i)
                i = re.sub(r'([0-9]*)[SW]', '-\\1', i)
            except TypeError, ValueError:
                # Either we found a plain int() or the first
                # substitution was already successfull, and the second
                # fails, which is both fine.
                pass
            finally:
                r.append(i)
        r.insert(0, ignore_me)
        return r

    def create_geolocation(self, sites, tname=None):
        '''Create (possibly multiple) Tasks to upload geolocations.'''
        name = 'geolocation upload'
        if tname: name = name + '({})'.format(tname)
        # Our translator creates this funny format, deal with it.
        content = [self.__check_coords(i['nd_geolocation.description'],
                                       i['nd_geolocation.latitude'],
                                       i['nd_geolocation.longitude'],
                                       i['nd_geolocation.altitude'])
                    for i in sites]
        args = ('nd_geolocation', content, self.GEOLOC_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        f = self.chado.insert_into
        t = Task(name, f, *args, **kwargs)
        return (t,)

    def create_stockprop(self, props, ptype='', tname=None):
        '''Create (possibly multiple) Tasks to upload stocks.

        Pass props = [['name0', 'val0'], ['name1', 'val1'], ...]
        <ptype> is a string, that meanst one call per property
        '''
        # get the stockprop type_id, possibly create it first
        if not ptype:
            raise RuntimeError('You need to specify a stockprop type.')
        where = "name = '{}'".format(ptype)
        try:
            type_id = self.chado.get_cvterm(where=where)[0].cvterm_id
        except IndexError:
            ts = self.create_cvterm([ptype], definition=['stockprop'])
            for t in ts: t.execute()
            type_id = self.chado.get_cvterm(where=where)[0].cvterm_id

        # get stock_ids joined with the values
        sql = "SELECT stock_id,uniquename FROM stock WHERE uniquename = {}"
        vals = ','.join("'{}'".format(p[0]) for p in props)
        where = 'ANY(ARRAY[{}])'.format(vals)
        sql = sql.format(where)
        self.chado.c.execute(sql)
        stocks = sorted(self.chado.c.fetchall(), key=lambda x: x[1])
        props = sorted(props, key=lambda x: x[0])
        if not len(stocks) == len(props):
            raise RuntimeError('unequal stocks/stockprops, unlikely')
        join = zip(stocks, props)
        content = [[sck[0],type_id,prp[1]] for sck,prp in join]
        #               ^ stock_id     ^ value
        name = 'stockprop upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('stockprop', content, self.STOCKPROP_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        t = Task(name, f, *args, **kwargs)
        return (t,)

    def __t1_create_stockprop(self, other, tname):
        t_stockprop = [Task.init_empty()]
        for stockprop in self.STOCKPROPS:
            if other[0].has_key(stockprop):
                props = [i[stockprop] for i in other]
                t = self.create_stockprop(props, stockprop, tname=stockprop)
                t_stockprop.append(t)
        return t_stockprop

    def __construct_rnd(self, i, v):
        s = '{0}_{1}_{2}'
        return s.format(i,v,randint(1024,1048576))

    def __t2_create_phenoes(self, stocks, descs, tname):
        t_phenos = [Task.init_empty()] # all in parallel!
        for descriptor in descs.keys():
            content = []
            name = 'phenotype upload'
            if tname: name = name + '({})'.format(tname)
            name = name + '({})'.format(descriptor)
            attr_id = self.__get_or_create_cvterm(descriptor).attr_id
            values = [i[descriptor] for i in descs]
            uniqs = [self.__construct_rnd(attr_id, i) for i in values] # TODO rly unique?
            content = [[n, attr_id, v] for n, v in zip(uniqs, values)]
            args = ('phenotype', content, self.PHENO_COLS)
            kwargs = {'cursor' : self.con.cursor()}
            if tname: name = name + '({})'.format(tname)
            t_phenos.append(Task(name, f, *args, **kwargs))
        return t_phenos

    def __t3_create_experiments(self, others, stocks):
        gs = []
        for i in others:
            if hasattr(i, 'site_name'):
                n = i['site_name']
            else:
                n = 'Not Available'
            gs.append(n)
        geo_n_stock = ["('{0}','{1}')".format(i,j) for i,j in zip(gs,stocks)]
        stmt = '''
            SELECT g.nd_geolocation_id,s.stock_id FROM (VALUES {}) v
                JOIN nd_geolocation g ON v.column1 = g.description
                JOIN stock s ON v.column2 = stock.uniquename
        '''
        stmt = stmt.format(','.join(geo_n_stock))

        def join_func(_, stmt_res):
            return stmt_res # geolocation_id,stock_id

        name = 'cvterm upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        insert_args = ('nd_experiment', [], self.EXP_COLS)
        insert_kwargs = {'cursor'  : self.con.cursor()}
        args = (stmt, join_func, insert_args, insert_kwargs)
        kwargs = {}
        t = Task(name, f, *args, **kwargs)
        return (t,)

    def __t4_link(self, stocks):
        stmt = '''
            SELECT {result_columns} FROM (VALUES {values}) v
                JOIN stock s ON v.column1 = stock.uniquename
                JOIN stockprop sp ON v.column2
                JOIN phenotype p ON ?
                JOIN nd_experiment e ON ?
                ?
        ''' #XXX
        values = None#XXX
        columns = 's.stock_id,sp.stockprop_id,p.phenotype_id,e.nd_experiment_id'
        stmt = stmt.format(result_columns=columns, values=values)

        name = 'link all the things'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        return (Task.init_empty(),)

    @staticmethod
    def link_all():
        pass

    def create_phenotype(self, stocks, descs, others=[], genus=None,
                         tname=None):
        '''
        Create (possibly multiple) Tasks to upload phenotypic data.

        stocks - list of stock names
        descs  - list of dict's of phenotypic data
        others - list of dict's with additional information
                 e.g.: geolocation(nd_geolocation), date's(stockprop), ..
        '''
        # === Plan of Action ===
        # T1   - create_stockprop from <others>
        t_stockprop = self.__t1_create_stockprop(others, tname)

        # T1.X - create_X from <others>
        #
        # T2   - upload phenotypes from <descs>
        t_phenos = self.__t2_create_phenoes(stocks, descs, tname)

        # T3   - get geolocation ids        # has already been uploaded..
        # T3   - create_nd_experiment
        t_experiment = self.__t3_create_experiments(others, stocks)

        # T4   - get stock ids
        # T4   - get stockprop ids
        # T4   - get phenotype ids
        # T4   - get nd_experiment ids
        # T4   - link all the things
        t_link = self.__t4_link(stocks)

        return ([t_stockprop, t_phenos, t_experiment], t_link)

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
