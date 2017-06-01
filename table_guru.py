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
        def col_equal(ora, chad):
            for a,b in conf.iteritems():
                try: # XXX print what fails here! ('RAICES_COMERCIALES')
                    if getattr(ora, a) != getattr(chad, b.split('.')[1]):
                        return False
                except AttributeError:
                    continue
            return True
        def col_in(ora, chad_list):
            for c in chad_list:
                if col_equal(ora, c):
                    return True
            return False
        return col_equal, col_in, conf, c_conf

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

            unknown = [i for i in self.data if not is_in(i, current)]
        else:
            unknown = self.data

        # Blacklist contains oracle attributes, which we understand according
        # to ontology or configuration, but which don't exist the OracleDB.
        # This is most likely a mistake in the ontology or configuration.
        # So we better pass this list to someone who cares (and is able to fix
        # it).
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

        return self.linker.create_cvterm(self.dbname, self.cvname, cvt_ns,
                                         definition=cvt_ds, tname=f_ext)

    def __check_and_add_stocks(self):
        '''Tasks to upload the genexpression information.

        This functions refers to the chado 'stock' table.
        '''
        stocks = self.__get_needed_data('stock')

        if stocks:
            stocks = [(i['stock.uniquename'], i['stock.name']) for i in stocks]
            orga = self.chado.get_organism(where="common_name = 'Cassava'")[0]
            germpl_t = GERMPLASM_TYPE # < TODO remove hardcoding   ^
            return self.linker.create_stock(stocks, germpl_t, orga)

    def __check_and_add_sites(self):
        '''Creates MCL spreadsheets to upload the geolocation information.

        This functions refers to the chado 'nd_geolocation' table.
        '''
        sites = self.__get_needed_data('nd_geolocation')

        if sites:
            mandatory_cvts = ['type', 'country', 'state', 'region', 'address',
                              'site_code']
            t1 = self.__check_and_add_cvterms(mandatory_cvts, f_ext='pre_sites')
            t2 = self.linker.create_geolocation(sites)
            return (t1, t2)
            # We don't need to split the data anymore, cuz no spreadsheed
            #names = [i['nd_geolocation.description'] for i in sites]
            #alts = [i['nd_geolocation.altitude'] for i in sites]
            #lats = [i['nd_geolocation.latitude'] for i in sites]
            #longs = [i['nd_geolocation.longitude'] for i in sites]
            #t2 = self.linker.create_geolocation(names, alts, lats, longs)

    def __check_and_add_contacts(self):
        '''Creates MCL spreadsheets to upload the contact information.

        This functions refers to the chado 'contact' table.
        '''
        tasks = []

        contacts = self.__get_needed_data('contact')
        if contacts:
            names = [i['contact.name'] for i in contacts]
            types = [i['contact.type_id'] for i in contacts]

        return tasks

    # TODO use 'pick_date' and 'plant_date'
    def __check_and_add_phenotypes(self):
        '''Creates MCL spreadsheets to upload the phenotyping data.

        This functions refers to the chado 'phenotype' table.
        Ontology comes into the playground here. (self.onto, ..)
        '''
        phenotypic_data, raw_data = \
            self.__get_needed_data('phenotype', mapping='oracle', raw=True)
        if not phenotypic_data:
            return []

        # Get metadata, we need to link.
        tr_inv = utility.invert_dict(self.tr)

        # stocks needs to be passed as aditional argument
        stocks = [getattr(i, tr_inv['stock.name']) for i in raw_data]

        # === Plan of Action ===
        # - remove <others>
        # - create_geolocation
        # - create_stockprop
        # - create_cvterm

        others = []
        for i in raw_data:
            new = {}
            for k,v in tr_inv.iteritems():
                if k == 'nd_geolocation.description':
                    # If this block throws an error one day remember:
                    #   a description is useless without the coordinates
                    name = getattr(i, tr_inv['nd_geolocation.description'])
                    alt = getattr(i, tr_inv['nd_geolocation.altitude'])
                    lat = getattr(i, tr_inv['nd_geolocation.latitude'])
                    lon = getattr(i, tr_inv['nd_geolocation.longitude'])
                    location = "{0}_{1}_{2}_{3}".format(name, alt, lat, lon)
                    new.update({'site_name' : location})
                if 'date' in k:
                    d = self.__tostr(getattr(i, v))
                    if 'plant' in k:
                        new.update({'plant_date' : d})
                    elif 'pick' in k:
                        new.update({'pick_date' : d})
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
                    new.update({'#'+t_name:v})
                else:
                    attr_blacklist.append(k)
            descs.append(new)
        if attr_blacklist:
            msg = '[blacklist:{tab}] Consider fixing these entries in the'\
                + ' config file or the ontology'\
                + ' tables:\n\'\'\'\n{blk}\n\'\'\'\n'
            self.qprint(msg.format(tab=self.table, blk=attr_blacklist))

        # Check if all phenotypes exist already as cvterm, if not, add 'em.
        pheno_cvts = [i[1:] for i in descs[0].keys()] # strip the '#'
        tasks += self.__check_and_add_cvterms(pheno_cvts, f_ext='pre_pheno')

        # Looking in the first ontology mapping and then Chado, to find the
        # genus of Cassava. ('Manihot')
        crp = next(self.onto.mapping.iteritems())[1][0].CROP
        where = "common_name = '{}'".format(crp)
        genus = self.chado.get_organism(where=where)[0].genus

        fname = os.path.join(self.basedir, 'phenotyping_data.xlsx')
        self.linker.create_phenotype(fname, self.dataset, stocks, descs,
                                     other=others, genus=genus)
        tasks.append(fname)
        return tasks

    def __get_trait_name(self, trait):
        '''Using self.onto.mapping, we create and return a nice trait name.
        
        If we get a trait name, that does not exist in the Ontology, an empty
        string is returned.
        '''
        if not self.onto.mapping.has_key(trait):
            return ''
        vonto = self.onto.mapping[trait]
        if len(vonto) > 1:
            msg = '[.__get_trait_name] Note: we dont use all information we'\
                + ' found.'
            self.vprint(msg)
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

    def create_upload_tasks(self, test=None):
        '''Multiplexer for the single rake_{table} functions.

        Each create necessary workbooks for the specified table, save them and
        returns all their names in an array.
        '''
        self.tr = self.get_translation()
        sql = OSQL.get_all_from.format(table=self.table)
        self.data = self.oracle.get_rows(sql, table=self.table,
                                         fetchamount=test)
        t = {}
        t.update({'stocks' : self.__check_and_add_stocks()})
        t.update({'sites' : self.__check_and_add_sites()})
        t.update({'contacts' : self.__check_and_add_contacts()})
        t.update({'phenos' : self.__check_and_add_phenotypes()})

        # Replace None values with dummies.
        for k,v in t.itervalues():
            if v is None:
                t[k] = Task('Empty', lambda: None, [], {})

        # we are done with it
        del self.data

        # the tasks variable is explained in migration.py -> __parallel_upload
        tasks = (
            [ t['stocks'], t['sites'], t['contacts'], ],
            t['phenos'], 
        )
        return tasks

# Just fill in some empty dict()'s.
for table in TableGuru.ALL_TABLES:
    try:
        tmp = TableGuru.TRANS[table]
    except KeyError:
        TableGuru.TRANS[table] = dict()

