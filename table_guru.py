import utility
from utility import OracleSQLQueries as OSQL
import chado
import cassava_ontology
import ConfigParser
import os
import datetime

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
            raise RuntimeError(msg.format('cv', chado_cv))
        if not self.dbname in [i.name for i in self.chado.get_db()]:
            raise RuntimeError(msg.format('db', chado_db))
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

    def __get_config(self, chado_table=None, oracle_column=None):
        '''Returns only the (TRANS, TRANS_C) config that match the given
        arguments.'''
        if not self.TRANS or not self.TRANS_C:
            self.tr = self.get_translation()

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

    def __get_compare_f(self, table):
        '''Using the config file, we create and return compare functions for a
        given chado table:
            is_equal(oracle_item, chado_item)
            is_in(oracle_item, list(chado_item[, ...]))
        '''
        # TODO use c_conf
        conf, c_conf = self.__get_config(chado_table=table)
        if not conf:
            return None, None, None, None
        def is_eq(ora, chad):
            for a,b in conf.iteritems():
                try: # TODO print what fails here! ('RAICES_COMERCIALES')
                    #msg =  '[is_eq] {0}->{1} != {2}->{3} : returns {4}'
                    #print msg.format(a, getattr(ora, a), b, getattr(chad,
                    #                 b.split('.')[1]), getattr(ora, a) !=
                    #                 getattr(chad, b.split('.')[1]))
                    if getattr(ora, a) != getattr(chad, b.split('.')[1]):
                        return False
                except AttributeError:
                    continue
            return True
        msg, fmt, j = '[equal-comp : {0}] {1}', '({0} == {1})', ','
        it = conf.iteritems()
        msg = msg.format(table,
                         j.join(fmt.format(i,j.split('.')[1]) for i,j in it))
        self.vprint(msg)
        def is_in(ora, chad_list):
            for c in chad_list:
                if is_eq(ora, c):
                    return True
            return False
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
            msg = 'unknown <mapping> argument: {arg}, must be in {li}'
            msg = msg.format(arg=mapping, shouldbe=['chado', 'oracle'])
            raise RuntimeError(msg)

        # Get the config, and create according compare-functions.
        is_equal, is_in, trg, trg_c = self.__get_compare_f(tab)

        # comparing with phenotype.value doesn't make any sense, we would need
        # to:
        #   1. lookup attr_id->cvterm.name
        #   2. lookup stock_id->stock.name
        #   3. compare VARIEDAD with stock.name and attr_id with phenotype name
        # TODO make phenotype comparisons happen, will be performance heavy
        if tab == 'phenotype':
            def is_in(_, __):
                return False

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

            # It would be nice if is_in would throw a TypeError if the first
            # argument is not a OracleDB entry or the second one is not a list
            # of Chado entries.
            unknown = []
            for i in self.data:
                #msg = '[-] {0} {1} entry: {2}'
                #item = 'var:{0},gid:{1}'.format(getattr(i, 'VARIEDAD'),
                #                                getattr(i, 'GID'))
                #msg = msg.format('{}',tab,item)
                if not is_in(i, current) and not i in unknown:
                    unknown.append(i)
                    #self.vprint('[+] adding {0}: {1}'.format(tab,item))
                elif is_in(i, current): self.vprint(msg.format('known'))
                elif i in unknown: self.vprint(msg.format('double'))
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
            if raw: needed_data_raw.append(whole_entry)

            skip = True
            for ora_attr,cha_attr in trg.iteritems():
                if ora_attr in blacklist:
                    continue
                skip = False
                try:
                    # See __doc__
                    if mapping == 'chado':
                        entry.update(
                            {cha_attr : getattr(whole_entry, ora_attr)}
                        )
                    elif mapping == 'oracle':
                        entry.update(
                            {ora_attr : getattr(whole_entry, ora_attr)}
                        )
                except AttributeError:
                    blacklist.append(ora_attr)
            # Highly unlikely, but if it happens we better tell someone.
            if skip and raw:
                msg = ' skipped a hole data-line, while also returning raw'\
                    + ' data. This might lead to corrupting between 1 and'\
                    + ' every line after this one!'
                raise RuntimeError(msg)

            needed_data.append(entry)

        if blacklist:
            msg = '[blacklist:{tab}] Consider fixing these entries in the'\
                + ' config file or the ontology'\
                + ' tables:\n\'\'\'\n{blk}\n\'\'\'\n'
            self.qprint(msg.format(tab=tab, blk=blacklist))

        if raw: return needed_data, needed_data_raw
        return needed_data

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
            # TODO add aditional information, like METHOD_NAME,
            # METHOD_CLASS, SCALE_ID, TRAIT_DESCRIPTION, TRAIT_CLASS, ++
            # But therefor we need to access Chado manually.
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
            # Stocks might be unique tuples, BUT neither stock.name nor
            # stock.uniquename is completely unique in our database. Thus we
            # need to filter AGAIN..
            stock_ns = [i['stock.name'] for i in stocks]
            #stock_us = [i['stock.uniquename'] for i in stocks]
            stock_us = stock_ns
            orga = self.chado.get_organism(where="common_name = 'Cassava'")[0]
            germpl_t = GERMPLASM_TYPE # < TODO remove hardcoding   ^
            return self.linker.create_stock(stock_ns, orga,
                                            stock_uniqs=stock_us,
                                            germplasm_t=germpl_t)

    def __check_and_add_sites(self):
        '''Creates MCL spreadsheets to upload the geolocation information.

        This functions refers to the chado 'nd_geolocation' table.
        '''
        sites = self.__get_needed_data('nd_geolocation')
        self.vprint('[+] sites: {} rows'.format(len(sites)))

        if sites:
            # I don't know how this is possible but get duplicates at this point.
            sites = utility.uniq(sites)
            mandatory_cvts = ['type', 'country', 'state', 'region', 'address',
                              'site_code']
            t1 = self.__check_and_add_cvterms(mandatory_cvts, f_ext='pre_sites')
            t2 = self.linker.create_geolocation(sites)
            return (t1, t2)

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
        tr_inv = utility.invert_dict(self.tr)

        # stocks needs to be passed as aditional argument
        stocks = [getattr(i, tr_inv['stock.name']) for i in raw_data]

        others = []
        for i in raw_data:
            new = {}
            for k,v in tr_inv.iteritems():
                if k == 'nd_geolocation.description':
                    name = getattr(i, tr_inv['nd_geolocation.description'])
                    new.update({'site_name' : name})
                if 'date' in k:
                    d = self.__tostr(getattr(i, v))
                    if 'plant' in k:
                        new.update({'plant_date' : d})
                    elif 'pick' in k:
                        new.update({'pick_date' : d})
            if not new in others:
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

        t_phen = self.linker.create_phenotype(stocks, descs, others=others,
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

    def create_upload_tasks(self, max_round_fetch=1000, test=None):
        '''Multiplexer for the single rake_{table} functions.

        Each create necessary workbooks for the specified table, save them and
        returns all their names in an array.
        '''
        msg = '[create_upload_tasks] max_round_fetch={0}, test={1}'
        self.vprint(msg.format(max_round_fetch, test))
        self.tr = self.get_translation()

        if test and (test < max_round_fetch):
            max_round_fetch = test
        c_name = 'main_data'
        sql = OSQL.get_all_from.format(table=self.table)
        self.data = self.oracle.get_rows(sql, table=self.table,
                                         fetchamount=max_round_fetch,
                                         save_as=c_name)
        round_N = -1
        while True:
            round_N += 1
            print '[+] === upload round {} ==='.format(round_N)
            print '[+] data: {} rows'.format(len(self.data))

            t = {}
            t.update({'stocks' : self.__check_and_add_stocks()})
            t.update({'sites' : self.__check_and_add_sites()})
            t.update({'contacts' : self.__check_and_add_contacts()})
            t.update({'phenos' : self.__check_and_add_phenotypes()})

            # Replace None values with dummies.
            for k,v in t.iteritems():
                if v is None:
                    t[k] = Task('Empty', lambda: None, [], {})

            # the tasks variable is explained in migration.py -> __parallel_upload
            tasks = (
                [ t['stocks'], t['sites'], t['contacts'], ],
                t['phenos'], 
            )

            yield tasks

            print 'xxx', test, self.oracle.cur.rowcount
            if test:
                if self.oracle.cur.rowcount >= test:
                    break
                if (test - self.oracle.cur.rowcount) < max_round_fetch:
                    max_round_fetch = test - self.oracle.cur.rowcount
            self.data = self.oracle.fetch_more(n=max_round_fetch,
                                               from_saved=c_name)
            print 'yyy', self.data[:3]
            if not self.data:
                break

# Just fill in some empty dict()'s.
for table in TableGuru.ALL_TABLES:
    try:
        tmp = TableGuru.TRANS[table]
    except KeyError:
        TableGuru.TRANS[table] = dict()

