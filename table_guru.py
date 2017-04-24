import utility
from utility import OracleSQLQueries as OSQL
import chado
import spreadsheet
import cassava_ontology
import ConfigParser
import os

# Path to the translation cfg file.
CONF_PATH = 'trans.conf'

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
    
    def __init__(self, table, cursor, verbose=False):
        '''We initialize (once per session, nor per __init__ call!)
        TableGuru.COLUMNS such that:
            TableGuru.COLUMNS[<tablename>][0] -> first  column name
            TableGuru.COLUMNS[<tablename>][1] -> second column name..
            
        And TableGuru.TRANS with empty dict()'s.
        '''
        self.table = table
        self.c = cursor
        self.VERBOSE = verbose
        self.onto = cassava_ontology.CassavaOntology(self.c)
        self.chado = chado.ChadoPostgres()

        if not TableGuru.COLUMNS:
            for table in TableGuru.ALL_TABLES:
                self.c.execute(
                    OSQL.get_column_metadata_from.format(table_name=table)
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

        # NOTE its not necessary to check this on every __init__ right?
        for table in TableGuru.ALL_TABLES:
            try:
                tmp = TableGuru.TRANS[table]
            except KeyError:
                TableGuru.TRANS[table] = dict()
        
    def create_workbooks(self, update=False):
        '''Multiplexer for the single rake_{table} functions.
        
        Each create necessary workbooks for the specified table, save them and
        returns all their names in an array.
        '''
        to_call = 'rake_'+self.table.lower()
        func = getattr(self, to_call)
        if not func or not callable(func):
            msg = '[.create_workbooks] table: {0}, func: {1}'
            msg = msg.format(self.table, to_call)
            raise RuntimeError(msg)
        return func(update)

    def rake_vm_resumen_enfermedades(self, update=False):
        '''Create (and return as list of strings) spreadsheets to upload all
        data from the Oracle table: VM_RESUMEN_ENFERMEDADES'''
        tdict = self.get_translation()
        sprd = spreadsheet.MCLSpreadsheet(self.chado)

        raise NotImplementedError('''\
            # TODO: Writeon, but dont know when to create, because noone tells
            #       me what those columns mean..\
        ''')

        # Something like..
        #if need_create_db():
        #    sprd.create_db()
        #if need_create_cv():
        #    sprd.create_cv()

    def rake_vm_resumen_eval_avanzadas(self, update=False):
        '''Create (and return as string) spreadsheets to upload all data from
        the Oracle table: VM_RESUMEN_EVAL_AVANZADAS'''
        tdict = self.get_translation()
        sprd = spreadsheet.MCLSpreadsheet(self.chado)

        raise NotImplementedError('''\
            # TODO: Writeon, but dont know when to create, because noone tells
            #       me what those columns mean..\
        ''')

    def __check_column(self, table, conf, entry):
        '''Check a single entry in the currently parsed config for correcness.
        
        If we cannot handle <entry> for <table> we return False, otherwise True
        is returned.
        '''
        if not TableGuru.COLUMNS.has_key(table):
            # This is one of the ontology tables.. 
            # TODO Implement me maybe, but we can ignore this for now.
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

