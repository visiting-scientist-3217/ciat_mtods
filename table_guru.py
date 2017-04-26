import utility
from utility import OracleSQLQueries as OSQL
import chado
import spreadsheet
import cassava_ontology
import ConfigParser
import os

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
    
    def __init__(self, table, oracledb, verbose=False, basedir=''):
        '''We initialize (once per session, nor per __init__ call!)
        TableGuru.COLUMNS such that:
            TableGuru.COLUMNS[<tablename>][0] -> first  column name
            TableGuru.COLUMNS[<tablename>][1] -> second column name..
            
        And TableGuru.TRANS with empty dict()'s.
        '''
        self.table = table
        self.VERBOSE = verbose
        self.oracle = oracledb
        if not self.oracle.cur:
            self.oracle.connect()
        self.c = oracledb.cur
        self.onto = cassava_ontology.CassavaOntology(self.c)
        self.chado = chado.ChadoPostgres()
        self.basedir = basedir

        if not TableGuru.COLUMNS:
            for table in TableGuru.ALL_TABLES:
                self.c.execute(
                    OSQL.get_column_metadata_from.format(table=table)
                )
                # column_name is the 2nd entry of each tuple, see OSQL
                TableGuru.COLUMNS[table] = [
                    line[1] for line in self.c.fetchall()
                ]
                self.vprint(
                    '[+] TableGuru.COLUMNS[{table}] = {res}'.format(
                        table=table,
                        res=str(self.COLUMNS[table])[:30]+"... ]"
                    )
                )

    def __check_column(self, table, conf, entry):
        '''Check a single entry in the currently parsed config for correcness.
        
        If we cannot handle <entry> for <table> we return False, otherwise True
        is returned.
        '''
        if not TableGuru.COLUMNS.has_key(table):
            # This is one of the ontology tables.. We can ignore this for now.
            return True

        if entry in TableGuru.COLUMNS[table]:
            TableGuru.TRANS[self.table][entry] = conf.get(self.table, entry)
            return True
        if TableGuru.TRANS_C.has_key(entry):
            return True

        value = conf.get(self.table, entry)
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

    def __get_config(self, chado_table=None, oracle_table=None):
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

        if chado_table and oracle_table:
            tr, ctr = fill_with_cond("v.split('.')[0] == c and k == o",
                                     chado_table, oracle_table)
        if chado_table:
            tr, ctr = fill_with_cond("v.split('.')[0] == c", chado_table)
        elif oracle_table:
            tr, ctr = fill_with_cond("k == o", oracle_table)
        else:
            raise RuntimeError('Must supply either chado_table or oracle_table')

        return tr, ctr

    def __get_compare_f(self, table):
        '''Using the config file, we create and return compare functions for a
        given chado table:
            is_equal(oracle_item, chado_item)
            is_in(oracle_item, list(chado_item[, ...]))
        '''
        # TODO use c_conf
        conf, c_conf = self.__get_config(chado_table=table)
        def col_equal(ora, chad):
            for a,b in conf.iteritems():
                if getattr(ora, a) != getattr(chad, b.split('.')[1]):
                    return False
            return True
        def col_in(ora, chad_list):
            for c in chad_list:
                if col_equal(ora, c):
                    return True
            return False
        return col_equal, col_in

    def __check_and_add_stocks(self):
        '''Return a list() of created stock-spreadsheets. If none have to be
        added an empty list is returned.'''
        sheets = []

        # Get the config, and create according compare-functions.
        col_equal, col_in = self.__get_compare_f('stock')

        current_stocks = self.chado.get_stock()
        unknown_stocks = [i for i in self.data if not col_in(i, current_stocks)]
        stocks = [(i.GID, i.VARIEDAD) for i in unknown_stocks]

        if stocks:
            # i rly want to: self.chado.create_stock(..)
            orga = self.chado.get_organism(where="common_name = 'Cassava'")[0]
            fname = os.path.join(self.basedir, 'stocks.xlsx')
            germpl_t = GERMPLASM_TYPE
            self.sht.create_stock(fname, stocks, germpl_t, orga.genus,
                                  orga.species)
            sheets.append(fname)
            self.vprint('[+] adding {}'.format(fname))

        return sheets

    # TODO move all the config parsing in a separate class
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

    def create_workbooks(self, update=False, test=None):
        '''Multiplexer for the single rake_{table} functions.
        
        Each create necessary workbooks for the specified table, save them and
        returns all their names in an array.
        '''
        self.tr = self.get_translation()
        self.sht = spreadsheet.MCLSpreadsheet(self.chado)
        sql = OSQL.get_all_from.format(table=self.table)
        self.data = self.oracle.get_rows(sql, table=self.table,
                                         fetchamount=test)

        # Find an call custom function for each spreadsheet.
        to_call = 'rake_'+self.table.lower()
        f = getattr(self, to_call)
        if not f or not callable(f):
            msg = '[.create_workbooks] table: {0}, f: {1}'
            msg = msg.format(self.table, to_call)
            raise RuntimeError(msg)
        f(update)

        # Do the thing.
        sht_paths = []
        sht_paths += self.__check_and_add_stocks()
        #sht_paths += self.__check_and_add_?()

        return sht_paths

    def rake_vm_resumen_enfermedades(self, update=False):
        '''Create (and return as list of strings) spreadsheets to upload all
        data from the Oracle table: VM_RESUMEN_ENFERMEDADES

            #     : Writeon, but dont know when to create, because noone tells
            #       me what those columns mean..
        '''
        pass

    def rake_vm_resumen_eval_avanzadas(self, update=False):
        '''Create (and return as string) spreadsheets to upload all data from
        the Oracle table: VM_RESUMEN_EVAL_AVANZADAS

            #     : Writeon, but dont know when to create, because noone tells
            #       me what those columns mean..
        '''
        pass



# Just fill in some empty dict()'s.
for table in TableGuru.ALL_TABLES:
    try:
        tmp = TableGuru.TRANS[table]
    except KeyError:
        TableGuru.TRANS[table] = dict()

