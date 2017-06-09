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
from utility import uniq
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

class ChadoPostgres(object):
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
                                    'nd_geolocation', 'nd_experiment',
                                    'dbxref', 'stockprop']
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
        msg = '[+] insert_into->{0}\n\t({1})\n\t-> (VALUES {2} ...])'
        print msg.format(table, columns, str(values)[:60])
        if not type(values[0]) in [list, tuple]:
            msg = 'expected values = [[...], [...], ...]\n\tbut received: {}'
            msg = msg.format(str(values)[:50])
            raise RuntimeError(msg)
        if not cursor:
            raise RuntimeError('need cursor object')
        f = StringIO('\n'.join('\t'.join(str(v) for v in vs) for vs in values))
        cursor.copy_from(f, table, columns=columns)
        cursor.connection.commit()

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
        if not insert_kwargs.has_key('cursor'):
            raise RuntimeError('need cursor object')

        c = insert_kwargs['cursor']
        c.execute(fetch_stmt)
        fetch_res = c.fetchall()

        args = list(insert_args)
        # replaces values with the constructed join'ed ones
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

    def delete_cvterm(self, name, cv=None, and_dbxref=False):
        '''Deletes all cvterms, with given name and cv.'''
        cond = "name = '{0}'".format(name)
        if cv:
            cvs = self.get_cv(where="name = '{}'".format(cv))
            cv_id = cvs[0].cv_id
            cond = cond + " AND cv_id = {1}".format(name, cv_id)
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
            # we strip this prefix, because it comes like this from the Config
            ndg = 'nd_geolocation.'
            for k,v in keys.iteritems():
                if k.startswith(ndg): k = k.split(ndg)[1]
                cond = cond + " and {col} = '{val}'".format(col=k, val=v)
        self.__delete_where('nd_geolocation', cond)

    def delete_stockprop(self, val=None, type=None, keyval=None, keyvals=None,
                         del_attr=False):
        '''Deletes all stockprops, with given value/type, or both.
        
        If the stockprop to delete does not exist, we return silently.
        If <val> or <type> is given <keyval[s]> are ignored.
        if <del_attr> is True, we also delete the type of the stockprop
        (the cvterm).
            keyval = {a:b}
            keyvals = [{a:b}, {c:d}, ..]
        '''
        if val and type:
            keyvals = [{type : val}]
        elif val or type:
            if val:
                cond = "value = '{}'".format(val)
            else:
                t = self.get_cvterm(where="name = '{}'".format(type))
                if not t: return
                cond = "type_id = {}".format(t[0].cvterm_id)
            return self.__delete_where('stockprop', cond)
        if keyval:
            keyvals = [keyval]

        conds = []
        if del_attr:
            to_del = set()
        for kv in keyvals:
            for k,v in kv.iteritems():
                t = self.get_cvterm(where="name = '{}'".format(k))
                if not t: return
                cond = "(type_id = {}".format(t[0].cvterm_id)
                cond = cond + " AND value = '{}')".format(v)
                conds.append(cond)
                if del_attr:
                    to_del.update({k})
        if del_attr:
            for i in to_del:
                self.delete_cvterm(i)

        cond = ' OR '.join(conds)
        self.__delete_where('stockprop', cond)

    def delete_phenotype(self, keyval=None, keyvals=None, del_attr=False):
        '''Deletes all phenotypes, with given descriptors/values.
        
        if <del_attr> is True, we also delete the type of the phenotype (the
        cvterm).
            keyval = {a:b}
            keyvals = [{a:b}, {c:d}, ..]
        '''
        if keyval:
            if keyvals:
                keyvals.append(keyval)
            else:
                keyvals = [keyval]
        conds = []
        for kv in keyvals:
            k,v = next(kv.iteritems())
            t = self.get_cvterm(where="name = '{}'".format(k))
            if not t: return
            cond = "(attr_id = {}".format(t[0].cvterm_id)
            cond = cond + " AND value = '{}')".format(v)
            conds.append(cond)
        cond = ' OR '.join(conds)
        self.__delete_where('phenotype', cond)

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
    EXP_STOCK_COLS  = ['nd_experiment_id', 'stock_id', 'type_id']
    EXP_PHENO_COLS  = ['nd_experiment_id', 'phenotype_id']

    # implemented stockprop's which might occur in the phenotyping metadata
    # TODO put this in the config file!
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
        if content:
            name = 'dbxref upload'
            if tname: name = name + '({})'.format(tname)
            f = self.chado.insert_into
            args = ('dbxref', content, self.DBXREF_COLS)
            kwargs = {'cursor' : self.con.cursor()} # new cursor() for every Task
            t1_dbxref = Task(name, f, *args, **kwargs)
        else:
            t1_dbxref = Task.init_empty()

        # Note: Scary code. We don't know dbxref_id's but still want to use
        # the copy_from upload, or at least an execute query that uploads all
        # our data at once, and not upload 1 dbxref, 1 cvterm at a time.

        # cvterm names will be added later to <content> in order to ensure
        # correct ordering, because we don't know the dbxref_id yet
        if definition:
            content = [[cv_id, de] for c,de in zip(cvterm, definition)] 
        else:
            content = [[cv_id, ''] for c in cvterm]
        fstmt = '''\
            SELECT accession,dbxref_id
                FROM dbxref JOIN (VALUES {}) AS v ON v.column1 = dbxref.accession
        '''
        fstmt = fstmt.format(','.join("('{}')".format(v) for v in cvterm))
        def join_func(x, y):
            return [[i[0],i[1],j[0],j[1]] for i,j in zip(x,y)]

        name = 'cvterm upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        insert_args = ['cvterm', content, self.CVTERM_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [fstmt, join_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        t2_cvt = Task(name, f, *args, **kwargs)
        return (t1_dbxref, t2_cvt) # tuple = enforce sequencial execution

    def __get_or_create_cvterm(self, term, acs=None):
        '''Returns the cvterm named <term>, if it does not exist, we create it
        first.
        '''
        if not acs: acs = term
        try:
            cvterm = self.chado.get_cvterm(where="name = '{}'".format(term))[0]
        except IndexError:
            ts = self.create_cvterm([term], accession=[acs], tname='__get_or_create')
            for t in ts: t.execute()
            cvterm = self.chado.get_cvterm(where="name = '{}'".format(term))[0]
        return cvterm

    def create_stock(self, stock_names, organism, stock_uniqs=None,
                     tname=None, germplasm_t='cultivar'):
        '''Create (possibly multiple) Tasks to upload stocks.

        If stock_uniqs is not given, it will be set equal to stock_names.
        '''
        cvt = self.__get_or_create_cvterm(germplasm_t)
        type_id = cvt.cvterm_id
        o_id = organism.organism_id
        if not stock_uniqs:
            stock_uniqs = stock_names
        it = zip(stock_names, stock_uniqs)
        # need to make the name unique.. XXX do we rly need this?
        it = uniq(it, key=lambda x: x[0])
        # need to make the uniquename unique..
        #it = uniq(it, key=lambda x: x[1])
        content = [[o_id, sn, su, type_id] for sn,su in it]

        name = 'stock upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('stock', content, self.STOCK_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        t = Task(name, f, *args, **kwargs)
        return [t,]

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
        return [t,]

    def create_stockprop(self, props, ptype='', tname=None):
        '''Create (possibly multiple) Tasks to upload stocks.

        Pass props = [['name0', 'val0'], ['name1', 'val1'], ...]
        <ptype> is a string, that meanst one call per property
        '''
        # get the stockprop type_id, possibly create it first
        if not ptype:
            raise RuntimeError('You need to specify a stockprop type.')
        type_id = self.__get_or_create_cvterm(ptype).cvterm_id

        # get stock_ids joined with the values
        stmt = '''
            SELECT s.stock_id,s.uniquename FROM (VALUES {}) AS v
                JOIN stock s ON s.uniquename = v.column1
        '''
        values = ','.join("('{}')".format(p[0]) for p in props)
        stmt = stmt.format(values)

        def join_func(arg, stmt_res):
            type_id = arg[0]
            props = arg[1]
            stocks = sorted(stmt_res, key=lambda x: x[1])
            props = sorted(props, key=lambda x: x[0])
            if not len(stocks) == len(props):
                print 'sss', stocks
                print 'ppp', props
                raise RuntimeError('unequal stocks/stockprops, unlikely')
            join = zip(stocks, props)
            content = [[sck[0],type_id,prp[1]] for sck,prp in join]
            #               ^ stock_id     ^ value
            return content

        name = 'stockprop upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        content = [type_id, props]
        insert_args = ['stockprop', content, self.STOCKPROP_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [stmt, join_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        t = Task(name, f, *args, **kwargs)
        return [t,]

    def __t1_create_stockprop(self, stocks, others, tname):
        t_stockprop = []

        # -- arg.. FIXME? this is only a problem for the unittests..
        # because the spreadsheets wanted state, we went with it, and now that
        # bites us in the ..
        curr_stocks = self.chado.get_stock()
        curr_stock_names = [i.name for i in curr_stocks]
        stocks = [i for i in stocks if i in curr_stock_names]
        if not stocks:
            return [Task.init_empty()]
        # -- arg..

        for stockprop in self.STOCKPROPS:
            if others[0].has_key(stockprop):
                props = [[s, o[stockprop]] for s,o in zip(stocks,others)]
                t = self.create_stockprop(props, stockprop, tname=stockprop)
                t_stockprop.append(t)
        return t_stockprop

    def __construct_rnd(self, i, v):
        s = '{0}_{1}_{2}'
        return s.format(i,v,randint(1024,1048576))

    def __t2_create_phenoes(self, stocks, descs, tname=None):
        content = []
        attr_ids = {}
        for d,s in zip(descs, stocks):
            for descriptor, value in d.iteritems():
                if not attr_ids.has_key(descriptor):
                    new = self.__get_or_create_cvterm(descriptor).cvterm_id
                    attr_ids.update({descriptor : new})
                attr_id = attr_ids[descriptor]
                # FIXME is <uniq> unique enough?
                uniq = self.__construct_rnd(attr_id, value)
                # we need to query these later to obtain the phenotype_id
                self.pheno_uniquenames.append(uniq)
                content.append([uniq, attr_id, value])
        name = 'phenotype upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('phenotype', content, self.PHENO_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        return Task(name, f, *args, **kwargs)

    def __get_geo_names(self, others):
        gs = []
        for i in others:
            if hasattr(i, 'site_name'):
                n = i['site_name']
            else:
                n = 'Not Available'
            gs.append(n)
        return gs

    def __t3_create_experiments(self, others, stocks, tname=None):
        gs = self.__get_geo_names(others)
        geos = ["('{}')".format(s) for s in gs]
        stmt = '''
            SELECT g.nd_geolocation_id FROM (VALUES {}) AS v
                JOIN nd_geolocation g ON v.column1 = g.description
        '''
        stmt = stmt.format(','.join(geos))
        def join_func(type_id, stmt_res):
            return map(lambda x: [x[0], type_id], stmt_res)
            # [type_id, geolocation_id]
        type_id = self.__get_or_create_cvterm('phenotyping').cvterm_id

        name = 'nd_experiment upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        insert_args = ['nd_experiment', type_id, self.EXP_COLS]
        insert_kwargs = {'cursor'  : self.con.cursor()}
        args = [stmt, join_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        t = Task(name, f, *args, **kwargs)
        return [t,]

    def __t4_link(self, others, stocks, tname=None):
        linkers = []

        stmt = '''
            SELECT e.nd_experiment_id,j.{result} FROM (VALUES {values}) AS v
                JOIN {join} j ON v.column1 = j.{join_col}
                JOIN nd_geolocation g ON v.column2 = g.description
                JOIN nd_experiment e ON g.nd_geolocation_id = e.nd_geolocation_id
        '''
        geos = self.__get_geo_names(others)

        # -- stocks --
        values = ','.join("('{}', '{}')".format(s, g) for s,g in zip(stocks,geos))
        stock_stmt = stmt.format(result='stock_id', values=values,
                                 join='stock', join_col='uniquename')
        cvt = self.__get_or_create_cvterm('sample')
        type_id = cvt.cvterm_id
        def join_stock_func(type_id,stmt_res):
            return map(lambda x: [x[0], x[1], type_id], stmt_res)
            # [nd_experiment_id, stock_id, type_id]

        name = 'link stocks'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        insert_args = ['nd_experiment_stock', type_id, self.EXP_STOCK_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [stock_stmt, join_stock_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        linkers.append(Task(name, f, *args, **kwargs))

        # -- phenotypes --
        it = zip(self.pheno_uniquenames, geos)
        values = ','.join("('{}', '{}')".format(p, g) for p,g in it)
        pheno_stmt = stmt.format(result='phenotype_id', values=values,
                                 join='phenotype', join_col='uniquename')
        def join_pheno_func(_, stmt_res):
            return stmt_res

        name = 'link phenotypes'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        insert_args = ['nd_experiment_phenotype', None, self.EXP_PHENO_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [pheno_stmt, join_pheno_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        linkers.append(Task(name, f, *args, **kwargs))

        return linkers

    # FIXME: missleading name, we do so much more than just create phenotypeing
    #        entries..
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
        t_stockprop = self.__t1_create_stockprop(stocks, others, tname)

        # T1.X - create_X from <others>
        #
        # T2   - upload phenotypes from <descs>
        self.pheno_uniquenames = [] # used to later query the ids
        t_phenos = self.__t2_create_phenoes(stocks, descs, tname)

        # T3   - get geolocation ids        # has already been uploaded..
        # T3   - create_nd_experiment
        t_experiment = self.__t3_create_experiments(others, stocks)

        # T4   - get stock ids, and nd_experiment ids
        # T4   - get phenotype ids, and nd_experiment ids
        # T4   - link 'em
        t_link = self.__t4_link(others, stocks)
        del self.pheno_uniquenames

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
        'dbxref', 'stockprop', 'nd_experiment']:
    prefix = 'get_'
    newfget_name = prefix+table
    if not hasattr(ChadoPostgres, newfget_name):
        newf = copy_func(ChadoPostgres._ChadoPostgres__get_rows, newfget_name)
        newf.func_defaults = (table,'',False)
        newf.func_doc = ChadoPostgres._ChadoPostgres__get_rows\
                                     .__doc__.format(table=table)
        setattr(ChadoPostgres, prefix+table, newf)
# META-END
