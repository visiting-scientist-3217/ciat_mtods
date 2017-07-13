import utility
from utility import OracleSQLQueries as OSQL
from utility import get_uniq_id as uid
import chado
import cassava_ontology
import ConfigParser
import os
import datetime, time
from task_storage import TaskStorage

# Path to the translation cfg file.
CONF_PATH = 'trans.conf'
GERMPLASM_TYPE = 'cultivar' # constant(3 options) from MCL

class ThisIsBad(RuntimeError):     pass
class ThisIsVeryBad(RuntimeError): pass

class TableGuru(utility.VerboseQuiet):
    '''This guy understands those spanish Oracle databases.

    We get initialized with a table name, and are expected to fill some excel
    workbook, such that MainlabChadoLoader understands it.

    Keys in the TRANS and COLUMNS_DICT are table names from the
    Oracle database, and if existent, they return the corresponding Oracle ->
    Chado translation dictionary or the columns of the Oracle table
    rspectively.

    The TRANS_C dict contains constanst relationships, that have to be
    populated by asking chado.

    All __check_and_add_*-functions return a list() of created upload-tasks.
    If none have to be added an empty list is returned.
    '''

    TRANS = {}
    TRANS_C = {}
    COLUMNS = {}

    ALL_TABLES = [
        'VM_RESUMEN_ENFERMEDADES',
        'VM_RESUMEN_EVAL_AVANZADAS',
        'VM_RESUMEN_EVAL_CALIDAD',
        'VM_RESUMEN_EVAL_MOSCA_BLANCA',
    ]

    def __init__(self, table, oracledb, verbose=False, basedir='', update=True,
                 chado_db='mcl_pheno', chado_cv='mcl_pheno',
                 chado_dataset='mcl_pheno'):
        '''We initialize (once per session, nor per __init__ call!)
        TableGuru.COLUMNS such that:
            TableGuru.COLUMNS[<tablename>][0] -> first  column name
            TableGuru.COLUMNS[<tablename>][1] -> second column name..

        And TableGuru.TRANS with empty dict()'s.
        '''
        super(self.__class__, self).__init__()
        self.VERBOSE = verbose
        self.QUIET = False
        self.basedir = basedir
        self.update = update
        if not update:
            raise Warning('Deprecated flag: "update", only usefull for'\
                + ' spreadsheets.')

        self.oracle = oracledb
        if not self.oracle.cur:
            self.oracle.connect()
        self.c = oracledb.cur
        self.chado = chado.ChadoPostgres()
        self.onto = cassava_ontology.CassavaOntology(self.c)

        self.linker = chado.ChadoDataLinker(self.chado, chado_db, chado_cv)

        self.table = table
        self.dbname = chado_db
        self.cvname = chado_cv
        self.dataset = chado_dataset

        self.__error_checks()
        self.__setup_columns()


    def __error_checks(self):
        msg = 'TableGuru: Mandatory {0} not found: {1}'
        if not self.cvname in [i.name for i in self.chado.get_cv()]:
            raise RuntimeError(msg.format('cv', self.cvname))
        if not self.dbname in [i.name for i in self.chado.get_db()]:
            raise RuntimeError(msg.format('db', self.dbname))
        if not self.dataset in [i.name for i in self.chado.get_project()]:
            raise RuntimeError(msg.format('project/dataset', chado_dataset))

    def __setup_columns(self):
        if not TableGuru.COLUMNS:
            for table in TableGuru.ALL_TABLES:
                self.c.execute(
                    OSQL.get_column_metadata_from.format(table=table)
                )
                TableGuru.COLUMNS[table] = [
                    line[1] for line in self.c.fetchall()
                ]
                msg = '[+] TableGuru.COLUMNS[{table}] = {res}'
                msg = msg.format(table=table,
                                 res=str(self.COLUMNS[table])[:30]+"... ]")
                self.vprint(msg)

    def __tostr(self, d):
        '''Formatting helper'''
        if d == None:
            return ''
        if type(d) is datetime.datetime:
            return d.strftime('%Y-%m-%d')
        else:
            return str(d)

    def __check_column(self, table, conf, entry):
        '''Check a single entry in the currently parsed config for correcness.

        If we cannot handle <entry> for <table> we return False, otherwise True
        is returned.
        '''
        if not TableGuru.COLUMNS.has_key(table):
            # This is one of the ontology tables, no need for translation.
            return True

        if entry in TableGuru.COLUMNS[table]:
            TableGuru.TRANS[table][entry] = conf.get(table, entry)
            return True
        if entry.lstrip('_') in TableGuru.COLUMNS[table]:
            TableGuru.TRANS[table][entry] = conf.get(table, entry)
            self.vprint('Note: done lstrip(_) for "{}"'.format(entry))
            return True
        if TableGuru.TRANS_C.has_key(entry):
            return True

        value = conf.get(table, entry)
        if value[:2] == '/*' and value[-2:] == '*/':
            value = value.split('/*')[1].split('*/')[0]
            TableGuru.TRANS_C[entry] = value.lstrip().rstrip()
            return True

        print '[parser-error] {k} = {v}'.format(k=entry, v=value)
        return False

    def __parse_translation_config(self):
        '''We do extensive error checking, and might throw RuntimeError.'''
        conf = ConfigParser.RawConfigParser()

        if not os.path.exists(CONF_PATH):
            missing_file = 'TransConfig path does not exist: {}'
            raise RuntimeError(missing_file.format(CONF_PATH))
        conf.read(CONF_PATH)

        if not conf.has_section(self.table):
            missing_sec = 'TransConfig does not have section: {}'
            raise RuntimeError(missing_sec.format(self.table))

        for table in conf.sections():
            for column in conf.options(table):
                col = column.upper() # ConfigParser does .lower() somewhere..
                if not self.__check_column(table, conf, col):
                    msg = 'config entry could not be parsed: {0} = {1}'
                    msg = msg.format(col, conf.get(self.table, col))
                    raise RuntimeError(msg)

    def get_config(self, chado_table=None, oracle_column=None):
        '''Returns only the (TRANS, TRANS_C) config that match the given
        arguments.'''
        if not self.TRANS or not self.TRANS_C or not hasattr(self, 'tr'):
            self.tr = self.get_translation()
            self.tr_inv = utility.invert_dict(self.tr)

        def fill_with_cond(cond, c=None, o=None):
            tr = {}
            ctr = {}
            t_it = self.tr.iteritems()
            ct_it = self.TRANS_C.iteritems()
            [tr.update({k:v}) for k,v in t_it if eval(cond)]
            [ctr.update({k:v}) for k,v in ct_it if eval(cond)]
            return tr, ctr

        if chado_table and oracle_column:
            tr, ctr = fill_with_cond("v.split('.')[0] == c and k == o",
                                     chado_table, oracle_column)
        if chado_table:
            tr, ctr = fill_with_cond("v.split('.')[0] == c", chado_table)
        elif oracle_column:
            tr, ctr = fill_with_cond("k == o", oracle_column)
        else:
            raise RuntimeError('Must supply either chado_table or oracle_column')

        return tr, ctr

    def create_equal_comparison(self, table, conf, c_conf):
        '''Returns functions to compare oracle objects with chado objects.

        Functions are created only for the given table, using the given config.
        Return-order is:
            is_eq, is_in, conf, c_conf
        '''
        f,f2 = None,None

        if table == 'stock':
            TaskStorage.known_stock_ids = []
            TaskStorage.unknown_stocks = []
            conf_inv = utility.invert_dict(conf)
            def f(ora, chad):
                if getattr(ora, conf_inv['stock.name']) != chad.uniquename:
                    return False
                return True
            def f2(ora, chads):
                for c in chads:
                    if f(ora, c):
                        # it's equal, lets append its name to known names
                        TaskStorage.known_stock_ids.append(c.stock_id)
                        return True
                ora_stock_name = getattr(ora, conf_inv['stock.name'])
                if ora_stock_name in TaskStorage.unknown_stocks:
                    index = TaskStorage.unknown_stocks.index(ora_stock_name)
                    dup = utility.Duplicate(index)
                    TaskStorage.known_stock_ids.append(dup)
                else:
                    TaskStorage.unknown_stocks.append(ora_stock_name)
                    TaskStorage.known_stock_ids.append(None)
                return False

        elif table == 'stockprop':
            # need to update the mapping every round..
            stocks = self.chado.get_stock()
            m = {}
            for s in stocks: m.update({s.name : s.stock_id})
            self.map_stock_name_to_id = m

            m = {}
            sps = self.chado.get_stockprop()
            cvts = self.chado.get_cvterm()
            for sp in sps:
                spname = [i for i in cvts if i.cvterm_id == sp.type_id]
                assert(len(spname) == 1)
                spname = spname[0].name
                m.update({spname : sp.type_id})
            self.map_stockprop_type_to_cvterm_id = m

            def f(sp, current):
                m_stockid = self.map_stock_name_to_id
                m_propid = self.map_stockprop_type_to_cvterm_id
                s,p = sp
                if not m_stockid.has_key(s) or not m_propid.has_key(p):
                    return False
                s_id = m_stockid[s]
                p_id = m_propid[p]
                if [s_id, p_id] == current:
                    return True
                return False

            def f2(sp, currents):
                '''is_in([stock.name, property], [[id1,typ1],[..],..])

                Must be usable like:
                    ids = [[i.stock_id i.type_id] for i in chado.get_stockprop()]
                    f([stock,prop_t], ids)
                        -> True if stock,prop_t is already in ids
                '''
                m_stockid = self.map_stock_name_to_id
                m_propid = self.map_stockprop_type_to_cvterm_id
                s,p = sp
                if not m_stockid.has_key(s):
                    return False
                if not m_propid.has_key(p):
                    msg = 'No Key: Mapping(:stock_id => :cvterm_id) "{}"'
                    raise Warning(msg.format(p))
                s_id = m_stockid[s]
                p_id = m_propid[p]
                if [s_id, p_id] in currents:
                    return True
                return False

        else:
            def f(ora, chad):
                if chad in ora: return True
                return False
            def f2(ora, chads): #set
                if ora.intersection(chads): return True
                return False

        return f,f2

    def __get_compare_f(self, table):
        '''Using the config file, we create and return compare functions for a
        given chado table:
            is_equal(oracle_item, chado_item)
            is_in(oracle_item, list(chado_item[, ...]))
        '''
        # TODO[6] use c_conf
        conf, c_conf = self.get_config(chado_table=table)
        if not conf: return None, None, None, None

        is_eq, is_in = self.create_equal_comparison(table, conf, c_conf)

        return is_eq, is_in, conf, c_conf

    def __get_needed_data(self, tab, mapping='chado', raw=False):
        '''Returns data to upload as a list() of dict()'s for chado table
        <tab>, a dict() which mapps oracle column to value.

        This method uses all the information we have by creating dynamic
        comparison functions based on: config files, chado and oracle
        connection.

        If <mapping> is set to 'oracle', then the returned dictionary returns
        OracleDB keys instead of chado ones. This is necessary for
        disambiguating phenotypes, as they all reference the 'phenotype.value'
        field.

        If <raw> is True, we return a second list() of dict()'s with the whole,
        unfiltered oracle table entrys.
        '''
        if not mapping in ['chado', 'oracle']:
            msg = 'unknown <mapping> argument: {0}, must be in {1}'
            raise RuntimeError(msg.format(mapping, ['chado', 'oracle']))

        is_equal, is_in, trg, trg_c = self.__get_compare_f(tab)

        if not trg and not trg_c:
            msg = '[{}] no CONFIG found => not uploading any related data'
            self.qprint(msg.format(tab))
            return []

        if self.update:
            if not is_equal or not is_in:
                msg = '[{}] no translation found => not uploading any related'\
                    + ' data'
                self.qprint(msg.format(tab))
                return []

            func_name = 'get_'+tab
            get_all_func = getattr(self.chado, func_name)
            if not get_all_func or not callable(get_all_func):
                msg = 'Chado.{} not found'
                raise NotImplementedError(msg.format(func_name))
            current = get_all_func()

            # Default to non-override
            # Both _override lists are only used for comparison
            data_override = self.data
            curr_override = current

            if tab == 'stockprop':
                curr_override = [[i.stock_id, i.type_id] for i in current]
                data_override = []
                for attr in dir(self.data[0]):
                    if self.tr.has_key(attr) and 'stockprop.' in self.tr[attr]:
                        for d in self.data:
                            data_override.append(
                                [getattr(d, self.tr_inv['stock.name']),
                                 self.tr[attr][len('stockprop.'):]]
                            )
            elif tab == 'stock':
                pass
            else:
                curr_override = set(p.uniquename for p in self.chado.get_phenotype())
                def tmp(d):
                    id = uid(d, self.tr_inv)
                    mkuniq = chado.ChadoDataLinker.make_pheno_unique #returns set
                    uniqnames = set(mkuniq(id, t) for t in self.pheno_traits)
                    return uniqnames
                data_override = [tmp(d) for d in self.data]

            unknown = []
            for entry,entry_ovrw in zip(self.data, data_override):
                if not is_in(entry_ovrw, curr_override) and not entry in unknown:
                    unknown.append(entry)
        else:
            unknown = self.data

        if len(unknown) != len(set(unknown)):
            if raw: eclass = ThisIsVeryBad
            else:   eclass = ThisIsBad
            msg = '[{}] len(unknown) != len(set(unknown))'.format(tab)
            raise eclass(msg)

        # Blacklist contains oracle attributes, which we understand according
        # to ontology or configuration, but which don't exist the OracleDB.
        # This is most likely a mistake in the ontology or configuration.
        # So we better pass this list to someone who is able to fix it.
        blacklist = []

        needed_data = []
        if raw: needed_data_raw = []
        for whole_entry in unknown:
            entry = {}

            skip = True
            for ora_attr,cha_attr in trg.iteritems():
                if ora_attr in blacklist:
                    continue
                try:
                    # See __doc__
                    value = getattr(whole_entry, ora_attr)
                    if value == None: # eq (null) in oracle
                        continue
                    if mapping == 'chado':
                        entry.update(
                            {cha_attr : value}
                        )
                    elif mapping == 'oracle':
                        entry.update(
                            {ora_attr : value}
                        )
                    skip = False
                except AttributeError as e:
                    blacklist.append(ora_attr)

            if not skip:
                needed_data.append(entry)
                if raw:
                    needed_data_raw.append(whole_entry)

        if blacklist:
            msg = '[blacklist:{tab}] Consider fixing these entries in the'\
                + ' config file or the ontology'\
                + ' tables:\n\'\'\'\n{blk}\n\'\'\'\n'
            self.qprint(msg.format(tab=tab, blk=blacklist))

        if raw: return needed_data, needed_data_raw
        return needed_data

    # TODO remove this function, and replace its only usage by
    #      __get_or_create_cvterm
    def __check_and_add_cvterms(self, maybe_known, f_ext=''):
        '''Check if <maybe_known> cvterm-names already exist in Chado.

        We try to find information about the requested cvterms:
            -1- We check ontology. If we find information, we might create more
                than one cvterm per given cvterm-name, to comply to the chado
                relationship schema.
            -2- If not found(^), we create raw cvterms without description.

        Note that we do a case insensitive comparison while trying to find
        Ontology for a term in <maybe_known>.
        '''
        all_cvt_names = [i.name for i in self.chado.get_cvterm()]
        needed_cvts = [i for i in maybe_known if not i in all_cvt_names]
        tasks = []

        if not needed_cvts:
            return []
        needed_onto = []
        for cvt in needed_cvts:
            it = self.onto.mapping.iteritems()
            cur = [i[1][0] for i in it if i[1][0].TRAIT_NAME.lower() ==\
                    cvt.lower()]
            if len(cur) == 0:
                continue
            if len(cur) != 1: # Well crap.
                msg = 'Did not find excactly one cvterm, for a single needed'\
                    + ' name: {c} -> {d}'
                self.qprint(msg.format(c=cvt, d=cur))
                cur = [cur[0]]
            needed_onto += cur

        # Check if we have Ontology for all needed cvterms.
        if len(needed_cvts) == len(needed_onto):
            cvt_ns = [i.TRAIT_NAME for i in needed_onto]
            cvt_ds = ['{cls}: {dsc}'.format(dsc=i.TRAIT_DESCRIPTION,\
                        cls=i.TRAIT_CLASS) for i in needed_onto]
        else:
            cvt_ns = needed_cvts
            cvt_ds = []

        return self.linker.create_cvterm(cvt_ns, definition=cvt_ds,
                                         tname=f_ext)

    def __check_and_add_stocks(self):
        '''Tasks to upload the genexpression information.

        This functions refers to the chado 'stock' table.
        '''
        stocks = self.__get_needed_data('stock')
        self.vprint('[+] stocks: {} rows'.format(len(stocks)))

        if stocks:
            stock_ns = [i['stock.name'] for i in stocks]
            #stock_us = [i['stock.uniquename'] for i in stocks]
            stock_us = stock_ns
            orga = self.chado.get_organism(where="common_name = 'Cassava'")[0]
            germpl_t = GERMPLASM_TYPE # < TODO remove hardcoding   ^
            t = self.linker.create_stock(stock_ns, orga, stock_uniqs=stock_us,
                                         germplasm_t=germpl_t)
            return t



    def __check_and_add_stockprops(self):
        '''Tasks to upload the stockprop (stock-metadata).

        This functions refers to the chado 'stockprop' table.
        '''
        stockprops, raw = self.__get_needed_data('stockprop', raw=True)
        self.vprint('[+] stockprops: {} rows'.format(len(stockprops)))
        stocks = [getattr(i, self.tr_inv['stock.name']) for i in raw]

        if not stockprops:
            return
        t_stockprops = []
        for ora_attr,chad_attr in self.tr.iteritems():
            if not 'stockprop.' in chad_attr: continue
            prop_t = chad_attr[len('stockprop.'):]

            props = []
            fail_counter = 0
            for s,o in zip(stocks, stockprops):
                try:
                    props.append([s, o[chad_attr]])
                except KeyError:
                    fail_counter += 1

            t = self.linker.create_stockprop(props, prop_t, tname=prop_t)
            t_stockprops.append(t)
        return t_stockprops

    def __check_and_add_sites(self):
        '''Creates MCL spreadsheets to upload the geolocation information.

        This functions refers to the chado 'nd_geolocation' table.
        '''
        sites = self.__get_needed_data('nd_geolocation')

        if sites:
            sites = utility.uniq(sites) # needed! but don't know why
            self.vprint('[+] sites: {} rows'.format(len(sites)))
            mandatory_cvts = ['type', 'country', 'state', 'region', 'address',
                              'site_code']
            t1 = self.__check_and_add_cvterms(mandatory_cvts, f_ext='pre_sites')
            t2 = self.linker.create_geolocation(sites)
            return (t1, t2)
        else:
            self.vprint('[+] sites: {} rows'.format(len(sites)))

    def __check_and_add_contacts(self):
        '''Creates MCL spreadsheets to upload the contact information.

        This functions refers to the chado 'contact' table.
        '''
        tasks = []

        contacts = self.__get_needed_data('contact')
        self.vprint('[+] contacts: {} rows'.format(len(contacts)))
        if contacts:
            names = [i['contact.name'] for i in contacts]
            types = [i['contact.type_id'] for i in contacts]

        return tasks

    def __check_and_add_phenotypes(self):
        '''Creates MCL spreadsheets to upload the phenotyping data.

        This functions refers to the chado 'phenotype' table.
        Ontology comes into the playground here. (self.onto, ..)
        '''
        phenotypic_data, raw_data = \
            self.__get_needed_data('phenotype', mapping='oracle', raw=True)
        self.vprint('[+] phenotypes: {} rows'.format(len(phenotypic_data)))
        if not phenotypic_data:
            return []

        # Get metadata, we need to link.
        self.tr_inv = utility.invert_dict(self.tr)

        # stocks needs to be passed as aditional argument
        stocks = [getattr(i, self.tr_inv['stock.name']) for i in raw_data]
        ids = [uid(i, self.tr_inv) for i in raw_data]

        others = []
        for raw_entry in raw_data:
            new = {}
            for k,ora_attr in self.tr_inv.iteritems():
                if k == 'nd_geolocation.description':
                    name = getattr(raw_entry,
                                   self.tr_inv['nd_geolocation.description'])
                    new.update({'site_name' : name})
                if 'stockprop' in k:
                    value = self.__tostr(getattr(raw_entry, ora_attr))
                    new.update({k : value})
            others.append(new)

        # Get the real phenotyping data into position.
        descs = []
        attr_blacklist = []
        for phenos in phenotypic_data:
            new = {}
            for k,v in phenos.iteritems():
                if k in attr_blacklist:
                    continue
                t_name = self.__get_trait_name(k)
                if t_name:
                    new.update({t_name : v})
                else:
                    attr_blacklist.append(k)
            descs.append(new)
        if attr_blacklist:
            msg = '[blacklist:{tab}] Consider fixing these entries in the'\
                + ' config file or the ontology tables:\n'\
                + '\'\'\'\n{blk}\n\'\'\'\n'
            self.qprint(msg.format(tab=self.table, blk=attr_blacklist))

        # Note: We create needed cvterms for the descriptors syncronously on
        # Task.execution(), as their numbers are low.

        # Looking in the first ontology mapping and then Chado, to find the
        # genus of Cassava. ('Manihot')
        crp = next(self.onto.mapping.iteritems())[1][0].CROP
        where = "common_name = '{}'".format(crp)
        genus = self.chado.get_organism(where=where)[0].genus

        t_phen = self.linker.create_phenotype(ids, stocks, descs, others=others,
                                              genus=genus)
        return t_phen

    def __get_trait_name(self, trait):
        '''Using self.onto.mapping, we create and return a nice trait name.

        If we get a trait name, that does not exist in the Ontology, an empty
        string is returned.
        '''
        if not self.onto.mapping.has_key(trait):
            return ''
        vonto = self.onto.mapping[trait]
        if len(vonto) > 1:
            msg = 'Warning: We dont use all information we found.'
            self.qprint(msg)
        vonto = vonto[0]
        name = vonto.TRAIT_NAME
        return name

    def get_translation(self):
        '''Returns the translation dictionary for the current self.table.

        Note that we save that stuff in static class variables, thus after the
        first invocation, we don't access that file again.
        We also do extensive error checking of that config file.
        '''
        # There must be some manual configuration..
        if not TableGuru.TRANS[self.table]:
            self.__parse_translation_config()

        # And the ontology..
        TableGuru.TRANS[self.table].update(self.onto.get_translation())

        return TableGuru.TRANS[self.table]

    def create_upload_tasks(self, max_round_fetch=600000, test=None):
        '''Multiplexer for the single rake_{table} functions.

        Each create necessary workbooks for the specified table, save them and
        returns all their names in an array.
        '''
        msg = '[create_upload_tasks] max_round_fetch={0}, test={1}'
        self.vprint(msg.format(max_round_fetch, test))
        self.tr = self.get_translation()
        self.tr_inv = utility.invert_dict(self.tr)

        self.pheno_traits = []
        for ora,chad in self.tr.iteritems():
            if 'phenotype.value' == chad:
                self.pheno_traits.append(self.__get_trait_name(ora))

        if test and (test < max_round_fetch):
            max_round_fetch = test

        sql = OSQL.get_all_from
        primary_key_columns = uid(None, self.tr_inv, only_attrs=True)
        uniq_col = ', '.join(primary_key_columns)
        self.data = self.oracle.get_first_n(sql, max_round_fetch,
                                            table=self.table, ord=uniq_col)

        round_N = -1
        fetched = 0
        while True:
            round_N += 1
            fetched_cur = len(self.data)
            fetched += fetched_cur
            msg = '[+] === upload round {} ({}) ==='
            self.vprint(msg.format(round_N, time.ctime()))

            t = {}

            t.update({'stocks' : self.__check_and_add_stocks()})
            t.update({'stockprops' : self.__check_and_add_stockprops()})
            t.update({'sites' : self.__check_and_add_sites()})
            t.update({'contacts' : self.__check_and_add_contacts()})
            t.update({'phenos' : self.__check_and_add_phenotypes()})

            # Replace None values with dummies.
            for k,v in t.iteritems():
                if v is None:
                    self.vprint('[-] None-Task for {}'.format(k))
                    t[k] = utility.Task.init_empty()

            # the tasks variable is explained in migration.py -> Task.parallel_upload
            tasks = (
                [ t['stocks'], t['sites'], t['contacts'], ],
                [ t['phenos'], t['stockprops'] ],
            )
            yield tasks

            if test and fetched >= test:
                break
            self.data = self.oracle.get_n_more(sql, max_round_fetch,
                                               offset=fetched,
                                               table=self.table,
                                               ord=uniq_col)
            if not self.data:
                break
        self.vprint('[+] === the end ({}) ==='.format(time.ctime()))

# Just fill in some empty dict()'s.
for table in TableGuru.ALL_TABLES:
    try:
        tmp = TableGuru.TRANS[table]
    except KeyError:
        TableGuru.TRANS[table] = dict()

