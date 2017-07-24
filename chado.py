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
import utility
from StringIO import StringIO
from itertools import izip_longest as zip_pad
import random
from task_storage import TaskStorage
from collections import Counter

DB=''
USER='cassghub'
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
    def insert_into(table, values, columns=None, cursor=None, store=None):
        '''INSERT INTO <table> (columns) VALUES (values)
        
        kwargs:
            columns     specify columns in values, default to all columns of
                        table
            cursor      needed
            store       if given must be of the form [a,b] will store the
                        RETURNING column <a> of the insert statement into
                        TaskStorage.b
        '''
        if len(values) == 0:
            print '[Warning] No values to upload for {}'.format(table)
            if store:
                setattr(TaskStorage, store[1], [])
            return
        if not type(values[0]) in [list, tuple]:
            msg = 'expected values = [[...], [...], ...]\n\tbut received: {}'
            msg = msg.format(str(values)[:50])
            raise RuntimeError(msg)
        if not cursor:
            raise RuntimeError('need cursor object')

        w = values
        if not store:
            # default to using fileIO
            f = StringIO('\n'.join('\t'.join(str(i) for i in v) for v in w))
            cursor.copy_from(f, table, columns=columns)
            cursor.connection.commit()
        else:
            # making VALUE-format: ('a', 'b'), ('c', ..), ..
            c, p, q = ',', "({})", "'{}'"
            f = c.join(p.format(c.join(q.format(i) for i in v)) for v in w)
            sql = '''
                INSERT INTO {table} {columns} VALUES {values} RETURNING {what}
            '''
            columns = p.format(','.join(columns))
            sql = sql.format(table=table, columns=columns, values=f, what=store[0])
            cursor.execute(sql)
            res = cursor.fetchall()
            setattr(TaskStorage, store[1], res)

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

    @staticmethod
    def storage_y_insert_into(values_constructor, *insert_args,
                              **insert_kwargs):
        '''Construct insert_into content with the <values_constructor> and the
        global TaskStorage object.
        '''
        if not insert_kwargs.has_key('cursor'):
            raise RuntimeError('need cursor object')
        args = list(insert_args)
        args[1] = values_constructor(args[1], TaskStorage)
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

    def delete_stock(self, stock, column='name'):
        cond = column+" = '{}'".format(stock)
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
        if not any([val,type,keyval]): return
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

    def delete_phenotype(self, uniquename=None, keyval=None, keyvals=None,
                         del_attr=False, del_nd_exp=False):
        '''Deletes all phenotypes, with given descriptors/values.
        
        if <uniquename> is given we delete exactly that phenotype
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
        elif uniquename:
            cond = "uniquename = '{}'".format(uniquename)

        pheno = self.get_phenotype(where=cond)
        nphenoes = len(pheno)
        msg = 'ambiguous deletion: {}'
        if nphenoes > 1: raise RuntimeError(msg.format(nphenoes))
        if nphenoes == 0: return
        pheno = pheno[0]

        if del_attr:
            attr_cond = "cvterm_id = {}".format(pheno.attr_id)
            attr = self.get_cvterm(where=attr_cond)
        if del_nd_exp: self.delete_nd_experiment(phenotype=pheno)
        self.__delete_where('phenotype', cond)

    def delete_nd_experiment(self, phenotype=None, stock=None):
        if phenotype:
            sql = '''
                SELECT e.nd_experiment_id,ep.phenotype_id
                    FROM nd_experiment AS e, nd_experiment_phenotype AS ep
                    WHERE e.nd_experiment_id = ep.nd_experiment_id
                        AND ep.phenotype_id = {}
            '''.format(phenotype.phenotype_id)
            r = self.__exe(sql)
            if len(r) == 0: return
            if len(r) > 1:
                msg = 'Ambiguous Phenotype deletion. id:{}, name:{}'
                raise Warning(msg.format(r, phenotype.phenotype_id))
            exp_id = r[0][0]
            cond = "nd_experiment_id = {}".format(exp_id)
        elif stock:
            raise NotImplementedError('use phenotype')
        self.__delete_where('nd_experiment', cond)

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

    def __init__(self, chado, dbname, cvname, onto=None):
        self.db = dbname
        self.cv = cvname
        self.chado = chado
        self.con = chado.con
        self.c = self.con.cursor()
        self.onto = onto

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

        # Note: This is ugly.. we omitted the definition finding in the
        # TableGuru, now we have to insert it here..
        if not definition and self.onto:
            definition = []
            fmt = 'Trait Description: {0}; Method Description: {1}'
            get_desc = lambda x: fmt.format(x[0].TRAIT_DESCRIPTION,
                                            x[0].METHOD_DESCRIPTION)
            get_trait = lambda x: x[0].TRAIT_NAME
            for cvt in cvterm:
                it = self.onto.mapping.iteritems()
                t = [get_desc(j) for _,j in it if get_trait(j) == cvt]
                try:
                    idef = t[0]
                except IndexError:
                    idef = ''
                definition.append(idef)

        # Note: Scary code. We don't know dbxref_id's but still want to use
        # the copy_from upload, or at least an execute query that uploads all
        # our data at once, and not upload 1 dbxref, 1 cvterm at a time.
        # Cvterm names will be added later to <content> in order to ensure
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

    # TODO add definition
    #      including: TRAIT_NAME, TRAIT_DESCRIPTION
    #      maybe also : METHOD_NAME, METHOD_CLASS, SCALE_ID, TRAIT_CLASS, ++
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
        # need to make the name unique.. 
        it = uniq(it, key=lambda x: x[0])
        # need to make the uniquename unique..
        it = uniq(it, key=lambda x: x[1])
        content = [[o_id, sn, su, type_id] for sn,su in it]

        name = 'stock upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('stock', content, self.STOCK_COLS)
        kwargs = {'cursor' : self.con.cursor(),
                  'store' : ['stock_id', 'stock_ids']}
        t = Task(name, f, *args, **kwargs)
        return [t,]

    def __check_coords(self, _, lat, lon, alt):
        ''''''
        r = [_]
        for i in [lat, lon, alt]:
            try: 
                i = re.sub(r'^\s*0*', '', i)
                i = re.sub(r'([0-9]*)[NE]', '+\\1', i)
                i = re.sub(r'([0-9]*)[SW]', '-\\1', i)
            except TypeError, ValueError:
                # Either we found a plain int() or the first substitution was
                # already successfull, and the second fails, which is both
                # fine.
                pass
            finally:
                if i in ['-','+','',None]: i = '0'
                r.append(i)
        return r

    def create_geolocation(self, sites, tname=None):
        '''Create (possibly multiple) Tasks to upload geolocations.'''
        name = 'geolocation upload'
        if tname: name = name + '({})'.format(tname)

        # counting for fun, last run got 2 fails for 300k+ rows
        fail_counter = 0 
        content = []
        for i in sites:
            try:
                content.append(
                    self.__check_coords(i['nd_geolocation.description'],
                                        i['nd_geolocation.latitude'],
                                        i['nd_geolocation.longitude'],
                                        i['nd_geolocation.altitude']))
            except KeyError:
                fail_counter += 1

        args = ('nd_geolocation', content, self.GEOLOC_COLS)
        kwargs = {'cursor' : self.con.cursor()}
        f = self.chado.insert_into
        t = Task(name, f, *args, **kwargs)
        return [t,]

    def __strip_0time(self, ps):
        '''strip time from datetime stockprops'''
        for p in ps:
            if hasattr(p[1], 'date'):
                if callable(p[1].date):
                    p[1] = p[1].date()
        return ps

    def create_stockprop(self, props, ptype='', tname=None):
        '''Create (possibly multiple) Tasks to upload stocks.

        Pass props = [['name0', 'val0'], ['name1', 'val1'], ...]
        <ptype> is a string, that meanst one call per property
        '''
        if not ptype: raise RuntimeError('no stockprop type')
        type_id = self.__get_or_create_cvterm(ptype).cvterm_id

        stmt = '''
            SELECT s.stock_id,s.name FROM (VALUES {}) AS v
                JOIN stock s ON s.name = v.column1
        '''
        values = ','.join("('{}')".format(p[0]) for p in props)
        stmt = stmt.format(values)

        props = self.__strip_0time(props)

        def join_func(content, stmt_res):
            type_id = content[0]
            props = content[1]
            stocks = sorted(stmt_res, key=lambda x: x[1])
            props = sorted(props, key=lambda x: x[0])
            if not len(stocks) == len(props):
                msg = 'unequal stocks/stockprops, unlikely\nstocks:{}\nprops:{}'
                raise RuntimeError(msg.format(stocks, props))
            join = zip(stocks, props)
            content = [[sck[0],type_id,prp[1]] for sck,prp in join]
            #               ^ stock_id     ^ value
            content = uniq(content, key=lambda x: x[0])#necessary
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

    @staticmethod
    def make_pheno_unique(id, name):
        s = '{0}__{1}'
        return s.format(id, name)

    def __t1_create_phenoes(self, ids, descs, tname=None):
        content = []
        attr_ids = {}
        for id,d in zip(ids, descs):
            for descriptor, value in d.iteritems():
                if not attr_ids.has_key(descriptor):
                    # We create cvterm syncronously, as their numbers are low.
                    new = self.__get_or_create_cvterm(descriptor).cvterm_id
                    attr_ids.update({descriptor : new})
                attr_id = attr_ids[descriptor]
                uniq = self.make_pheno_unique(id, descriptor)
                self.pheno_uniquenames.append(id)
                if value is None: value = ''
                content.append([uniq, attr_id, value])
        name = 'phenotype upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.insert_into
        args = ('phenotype', content, self.PHENO_COLS)
        kwargs = {'cursor' : self.con.cursor(),
                  'store'  : ['phenotype_id', 'phenotype_ids']}
        return Task(name, f, *args, **kwargs)

    def __get_geo_names(self, others):
        gs = []
        for i in others:
            if i.has_key('site_name'):
                n = i['site_name']
            else:
                n = 'Not Available'
            gs.append(n)
        return gs

    def __t2_create_experiments(self, ids, descs, others, stocks, tname=None):
        ld, lo, li = len(descs), len(others), len(ids)
        if ld != lo or ld != li:
            msg = 'descriptors({}) ?= others({}) ?= ids({})'
            raise RuntimeError(msg.format(ld, lo, li))

        gs = self.__get_geo_names(others)
        geos = ["('{0}')".format(s) for s in gs]
        stmt = '''
            SELECT g.nd_geolocation_id FROM (VALUES {}) AS v
                JOIN nd_geolocation g ON v.column1 = g.description
        '''
        stmt = stmt.format(','.join(geos))
        def join_func(type_id, stmt_res):
            return [[x[0], type_id] for x in stmt_res]
            # [geolocation_id, type_id]
        type_id = self.__get_or_create_cvterm('phenotyping').cvterm_id

        name = 'nd_experiment upload'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.fetch_y_insert_into
        insert_args = ['nd_experiment', type_id, self.EXP_COLS]
        insert_kwargs = {'cursor'  : self.con.cursor(),
                         'store'   : ['nd_experiment_id', 'nd_experiment_ids']}
        args = [stmt, join_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        t = Task(name, f, *args, **kwargs)
        return [t,]

    def __t3_link(self, others, stocks, tname=None):
        linkers = []

        # -- stocks --
        cvt = self.__get_or_create_cvterm('sample')
        type_id = cvt.cvterm_id
        def join_stock_func(type_id,ts):
            le, ls = len(ts.nd_experiment_ids), len(ts.stock_ids)
            if le != ls:
                # we fill in the holes in the known stock_ids with the new ones
                ls = len(ts.stock_ids)
                c = Counter(ts.known_stock_ids)
                lnone_n_dups = c[None] + c[utility.Duplicate]
                if c[None] > 0 and c[None] != ls:
                    msg = 'holes({})+dups({}) in known stocks(={}) != new'\
                        + ' stocks({})'
                    msg = msg.format(c[None], c[utility.Duplicate],
                                     lnone_n_dups, ls)
                    raise RuntimeError(msg)
                stock_ids = []
                new = iter(ts.stock_ids) # -> [(321,), (123,), ...]
                for known in ts.known_stock_ids: # -> [321, 123, None, Dup, ...]
                    if known.__class__ == utility.Duplicate:
                        duplicate = known
                        stock_ids.append(ts.stock_ids[duplicate.index])
                    else:
                        stock_ids.append([known] if known else next(new))
                ts.stock_ids = stock_ids

            it = zip(ts.nd_experiment_ids, ts.stock_ids)
            return [[eid[0], sid[0], type_id] for eid,sid in it]

        name = 'link stocks'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.storage_y_insert_into
        insert_args = ['nd_experiment_stock', type_id, self.EXP_STOCK_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [join_stock_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        linkers.append(Task(name, f, *args, **kwargs))

        # -- phenotypes --
        def join_pheno_func(ids, ts):
            # Earlier this meant that we might have failed parsing Ontology
            # and/or configuration, which is no longer a problem.
            #if len(ids) == len(set(ids)):
            #    raise Warning('Only one phenotype per data line? Unlikely!')
            eid_iter = iter(ts.nd_experiment_ids)
            pid_iter = iter(ts.phenotype_ids)
            test_iter = iter(ids)

            r = []
            try:
                last_id = next(test_iter)
                eid = next(eid_iter)
                pid = next(pid_iter)
            except StopIteration:
                return r
            while True:
                try:
                    # only if we have a new row id we go to the next experiment
                    r.append([eid[0], pid[0]])
                    new_id = next(test_iter)
                    if new_id != last_id: 
                        eid = next(eid_iter)
                    pid = next(pid_iter)
                    last_id = new_id
                except StopIteration:
                    break
            return r

        name = 'link phenotypes'
        if tname: name = name + '({})'.format(tname)
        f = self.chado.storage_y_insert_into
        insert_args = ['nd_experiment_phenotype', self.pheno_uniquenames, self.EXP_PHENO_COLS]
        insert_kwargs = {'cursor' : self.con.cursor()}
        args = [join_pheno_func]
        args += insert_args
        kwargs = {}
        kwargs.update(insert_kwargs)
        linkers.append(Task(name, f, *args, **kwargs))

        return linkers

    def create_phenotype(self, ids, stocks, descs, others=[], genus=None,
                         tname=None):
        '''
        Create (possibly multiple) Tasks to upload phenotypic data.

        stocks - list of stock names
        descs  - list of dict's of phenotypic data
        others - list of dict's with additional information
                 e.g.: geolocation(nd_geolocation), date's(stockprop), ..
        '''
        # === Plan of Action ===
        # T1   - upload phenotypes from <descs>
        self.pheno_uniquenames = [] # used to later query the ids
        t_phenos = self.__t1_create_phenoes(ids, descs, tname)

        # T2   - get geolocation ids        # has already been uploaded..
        # T2   - create_nd_experiment
        t_experiment = self.__t2_create_experiments(ids, descs, others, stocks)

        # T3   - get stock ids, and nd_experiment ids
        # T3   - link 'em
        # T3   - get phenotype ids, and nd_experiment ids
        # T3   - link 'em
        t_link = self.__t3_link(others, stocks)
        del self.pheno_uniquenames

        return ([t_phenos, t_experiment], t_link)

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
