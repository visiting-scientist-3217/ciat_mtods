import utility
from utility import OracleSQLQueries as OSQL
import spreadsheet
import cassava_ontology

# Path to the translation cfg file.
CONF_PATH = ''

class TableGuru(utility.VerboseQuiet):
    '''This guy understands those spanish Oracle databases.
    
    We get initialized with a table name, and are expected to fill some excel
    workbook, such that MainlabChadoLoader understands it.

    Keys in the TRANSLATION_DICT and COLUMNS_DICT are table names from the
    Oracle database, and if existent, they return the corresponding Oracle ->
    Chado translation dictionary or the columns of the Oracle table
    rspectively.
    '''

    TRANSLATION_DICT = {}
    COLUMNS = {}

    ALL_TABLES = [
        'VM_RESUMEN_ENFERMEDADES',
        'VM_RESUMEN_EVAL_AVANZADAS',
        'VM_RESUMEN_EVAL_CALIDAD',
        'VM_RESUMEN_EVAL_MOSCA_BLANCA',
    ]
    SPANISH_ONTOLOGY_TABLE = 'V_ONTOLOGY_SPANISH'
    ONTOLOGY_TABLE = 'V_ONTOLOGY'
    
    def __init__(self, table, cursor, verbose=False):
        '''We initialize (once per session, nor per __init__ call!)
        TableGuru.COLUMNS such that:
            TableGuru.COLUMNS[<tablename>][0] -> first  column name
            TableGuru.COLUMNS[<tablename>][1] -> second column name..
            
        And TableGuru.TRANSLATION_DICT with empty dict()'s.
        '''
        self.table = table
        self.c = cursor
        self.VERBOSE = verbose

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
                tmp = TableGuru.TRANSLATION_DICT[table]
            except KeyError:
                TableGuru.TRANSLATION_DICT[table] = dict()
        
    def create_workbooks(self):
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
        return func()

    def rake_vm_resumen_enfermedades(self):
        '''That ^ table.'''
        tdict = self.read_table_translation()
        sprd = spreadsheet.MCLSpreadsheet()
        ontology, ontology_spanish = self.get_ontologies()

        raise NotImplementedError('''\
            # TODO: Writeon, but dont know when to create, because noone tells
            #       me what those columns mean..\
        ''')

        #if need_create_db():
        #    sprd.create_db()
        #if need_create_cv():
        #    sprd.create_cv()

    def get_ontologies(self):
        '''Returns the two named tuples, with the Ontology data.'''
        ontology_spanish = cassava_ontology.get_tabledata_as_tuple(
            self.c, self.SPANISH_ONTOLOGY_TABLE
        )
        ontology = cassava_ontology.get_tabledata_as_tuple(
            self.c, self.ONTOLOGY_TABLE
        )
        return ontology, ontology_spanish

    def read_table_translation(self):
        '''Returns the translation dictionary for the current self.table.
        
        Note that we save that stuff in static class variables, thus after the
        first invocation, we don't access that file again.
        We also do extensive error checking of that config file.
        '''
        if not TableGuru.TRANSLATION_DICT:

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
                    if not column in TableGuru.COLUMNS[table]: # aiaiai
                        unknown_col = 'TransConfig contains unknown column: {}'
                        raise RuntimeError(unknown_col.format(column))
                    TableGuru.TRANSLATION_DICT[self.table][column] = \
                        conf.get(self.table, column)

        return TableGuru.TRANSLATION_DICT[self.table]

