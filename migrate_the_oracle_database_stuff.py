#!/usr/bin/python
'''\
MtODS -- _M_igrate _t_he _O_racle _D_atabase _S_tuff
To our Chado schema, by .. creating excel files, which are then parsed and
uploaded by MCL.

NOTE For production we should rewrite the 'Full Migration'-option, to start
threads executing the 'Single Table'-option, as locking all the objects
passed to the TableGuru might be more work and not necessarily faster.
'''

# Official libraries.
import ConfigParser # Reading the table translation file.
import optparse
import os
import sys

# Note: this v is not the official cx_Oracle library, but a convenient wrapper.
import cx_oracle

# Local stuff.
import drush
import spreadsheet

# Default values.
BASE_DIR = os.getcwd()
OUTFILE = '/test.xlsx'
CONF_FILENAME = '/trans.conf'
CONF_PATH = BASE_DIR + CONF_FILENAME

def main():
    '''Read the __doc__ string of this module.'''
    global CONF_PATH # args.. FIXME
    parser = optparse_init()
    o, args = parser.parse_args()

    # Needed for unittests to run their main().
    sys.argv = [''] 

    if args:
        print 'Whts dis? -> "%s" ??' % (args)
        exit(1)

    # Check user input for crap.
    mutally_exclusive_msg = 'options {0} and {1} are mutally exclusive'
    exclusive_opts = zip(
        [[o.single_table,'-s'], [o.verbose,'-v'], [o.do_update,    '-u']], 
        [[o.basedir,     '-b'], [o.quiet,  '-q'], [not o.do_upload,'-n']]
    )
    for a,b in exclusive_opts:
        if a[0] and b[0]:
            parser.error(mutally_exclusive_msg.format(a[1],b[1]))

    if o.drush_root:
        drush.Drush.DRUPAL_PATH = o.drush_root
    if o.basedir:
        drush.Drush.BASE_DIR = o.basedir
    if o.config_path:
        CONF_PATH = o.config_path
    drush.test_and_configure_drush()

    if o.only_unittests:
        run_unittests()

    migra = Migration(verbose=o.verbose, quiet=o.quiet)

    if o.do_update:
        migra.update(basedir=o.basedir)
    elif o.basedir or not o.single_table:
        migra.full(basedir=o.basedir, upload=o.do_upload)
    else:
        migra.single(o.single_table)

    return 0

def optparse_init():
    '''Return an initialized optparse.OptionParser object, ready to parse the
    command line.
    '''
    p = optparse.OptionParser(description=__doc__)
    p.add_option('-v', '--verbose', action='store_true', dest='verbose',
        help='hablamelo', metavar='', default=False)
    p.add_option('-q', '--quiet', action='store_true', dest='quiet',
        help='daemon silence', metavar='', default=False)
    p.add_option('-r', '--drush-root', action='store', type='string',
        dest='drush_root', help='root dir of drupal installation',
        metavar='<path>', default='')
    p.add_option('-c', '--config', action='store', type='string',
        dest='config_path', help=\
        'path to the table translation config (default: {})'\
        .format('<basedir>/'+CONF_FILENAME), metavar='<path>', default='')
    p.add_option('-u', '--update', action='store_true', dest='do_update',
        help='update: only get new data from Oracle', metavar='', default=True)

    test = optparse.OptionGroup(p, 'Test Options', 'Some Testting utility')
    test.add_option('-t', '--test', action='store_true', dest='only_unittests',
        help='only run testCases and exit', metavar='', default=False)
    test.add_option('-n', '--no-upload', action='store_false', dest='do_upload',
        help='do NOT upload the spreadsheed via drush (default: False)',
        metavar='', default=True)

    # Full migration options
    pg_all = optparse.OptionGroup(p, 'Full Migration', 'Options for a full'+\
        ' migration of all known tables to the Chado database.\nThis is '+\
        'the default operation.')
    pg_all.add_option('-b', '--basedir', action='store', type='string',
        dest='basedir', help='default to {}'.format(BASE_DIR),
        metavar='<path>', default=BASE_DIR)

    # Single table options
    pg_single = optparse.OptionGroup(p, 'Single Table Options', 'Only use a'+\
        ' single specified table instead of all implemented ones.\nThis '+\
        'gets enabled by supplying the [-s] option.')
    pg_single.add_option('-s', '--singletab', action='store', type='string',
        dest='single_table', help='specify only a single table to migrate',
        metavar='<table>', default='')

    p.add_option_group(pg_all)
    p.add_option_group(pg_single)
    p.add_option_group(test)
    return p

class VerboseQuiet():
    '''To be inherited from.'''
    def vprint(self, s):
        '''Only print stuff, if we are in verbose mode.'''
        if self.VERBOSE:
            print s
    def qprint(self, s):
        '''Only print stuff, if we are NOT in quiet mode.'''
        if not self.QUIET:
            print s

class Migration(VerboseQuiet):
    '''Handle for the migration task.

    Tasks are selected by following methods:
        .full()             migrate all known tables
            basedir=<default:$PWD>

        .single(<table>)    migrate <table>
            do_upload=<default:True>
            filename=<default:test.xlsx>

        .update(<since>)    update all known tables, since <since>
            basedir=<default:$PWD>
    '''

    # List of Tables, for which a Chado migration is implemented.
    TABLES_MIGRATION_IMPLEMENTED = [
        'VM_RESUMEN_ENFERMEDADES',
    ]
    TABLES_MIGRATION_NOT_IMPLEMENTED = [
        'VM_RESUMEN_EVAL_AVANZADAS',
        'VM_RESUMEN_EVAL_CALIDAD',
        'VM_RESUMEN_EVAL_MOSCA_BLANCA',
    ]

    # Default Chado DB and CV names.
    DB_NAME = 'mcl_pheno'
    CV_NAME = 'mcl_pheno'

    def __init__(self, verbose=False, quiet=False):
        '''We set some configuration, connect to the database, and create a
        local cursor object.

        Arguments:
            verbose     print lots of debug info
            quiet       daemon mode, be silent
        '''
        self.VERBOSE = verbose
        self.QUIET = quiet
        self.xlsx_files = []

        # NOTE We only need the db connection + cursor in this class to know
        # all table names, and we pass that cursor on to the TableGuru, to
        # avoid making another db connection.
        self.db = cx_oracle.Oracledb(pw='mruiz') # sneakily hardcoded password
        if self.VERBOSE: self.db.debug = True
        self.connection, self.cursor = self.db.connect()
        self.vprint('[+] connected')

    def update(self, basedir=BASE_DIR):
        '''NOT IMPLEMENTED'''
        print self.update.__doc__
        # TODO implementa me..

    def full(self, basedir=BASE_DIR, upload=True):
        '''We call the table migration task for all tables in
        TABLES_MIGRATION_IMPLEMENTED.

        Arguments:
            basedir     excel file storage location
            upload      perform upload task, after creating the excel files
        '''
        if not os.path.exists(basedir):
            raise RuntimeError('[.full] non existent path "{}"'\
                               .format(basedir))
        self.basedir = basedir
        self.do_upload = upload
        self.vprint('[+] basedir = "{0}", do_upload = {1}'.format(self.basedir,
            self.do_upload))

        for table in self.get_tables():
            if table in self.TABLES_MIGRATION_IMPLEMENTED:
                self.single(table, filename=self.basedir + table + '.xlsx')

    def single(self, table):
        '''Migrates a single table, including upload if specified.'''
        self.vprint('[+] starting migrate({})'.format(table))
        # Following call appends created filenames to self.xlsx_files
        self.create_xlsx_from(table)
        if self.do_upload:
            for xlsx_file in self.xlsx_files:
                self.upload(xlsx_file)

    def create_xlsx_from(self, table):
        '''Does excactly that.

        Note: We might create multiple worksheets, even when only migrating a
              single table. These MUST be uploaded in the given order.
        '''
        tg = TableGuru(table, self.cursor, self.VERBOSE)
        names = tg.create_workbooks()
        for name in names:
            self.xlsx_files += name

    def upload(self, filename=''):
        '''Upload the given xlsx file

        We upload by calling `drush` with some args.

        Note: We SHOULD bypass drush + MCL + Excel all along, and just put all
              mannually into the Chado tables.
        '''
        if not filename:
            raise RuntimeError('[.upload] no *.xlsx filename + no xlrd object')
        if filename[4:] != 'xlsx':
            self.qprint('[.upload] filename does not end in xlsx')

        for fname in self.xlsx_files:
            status, out = drush.Drush.execute(drush.Drush.MCL_UPLOAD, fname)
            if status != 0:
                qprint('[.upload] drush failed with: {0} {1}',
                    drush.Drush.MCL_UPLOAD, fname)

    def get_tables(self):
        self.cursor.execute(OracleSQLQueries.get_table_names)
        table_names = self.cursor.fetchall()
        if not table_names:
            raise RuntimeError('[.get_tables] failed to fetch table names')
        return table_names

class TableGuru(VerboseQuiet):
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
    
    def __init__(self, table, cursor, verbose=False):
        '''Initialize ONLY ONCE TableGuru.COLUMNS such that:
            TableGuru.columns[0][0] -> first  column name
            TableGuru.columns[1][0] -> second column name
            
        And TableGuru.TRANSLATION_DICT with empty dict()'s.
        '''
        self.table = table
        self.c = cursor
        self.VERBOSE = verbose

        if not TableGuru.COLUMNS:
            for table in Migration.TABLES_MIGRATION_IMPLEMENTED:
                self.c.execute(
                    OracleSQLQueries.get_column_name_type_length_where(
                        table_name=table))
                TableGuru.COLUMNS[table] = self.c.fetchall()
                vprint('[+] TableGuru.COLUMNS[{table}] = {res}'\
                    .format(table=table, res=TableGuru.COLUMNS[table]))

        # NOTE its not necessary to check this on every __init__ right?
        for table in Migration.TABLES_MIGRATION_IMPLEMENTED:
            try:
                tmp = TableGuru.TRANSLATION_DICT[table]
            except KeyError:
                TableGuru.TRANSLATION_DICT[table] = dict()
                vprint('[+] adding empty dict() for {table}'\
                    .format(table=table))
        
    def create_workbooks(self):
        '''Multiplexer for the single rake_{table} functions.
        
        Each create necessary workbooks for the specified table, save them and
        returns all their names in an array.
        '''
        func = None
        d = {'f' : func}
        exec 'f = self.rake_{}'.format(table.lower()) in d

        if not callable(self.func):
            msg = '[.create_workbooks] table: {0}, func: {1}'
            msg = msg.format(self.table, func)
            raise RuntimeError(msg)

        return self.func()

    def rake_vm_resumen_enfermedades(self):
        '''That ^ table.'''
        tdict = self.read_table_translation()

        sprd = spreadsheet.MCLSpreadsheet()
        if need_create_db():
            sprd.create_db()
        if need_create_cv():
            sprd.create_cv()
        # TODO: WRITEON

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

class OracleSQLQueries():
    '''Namespace for Oracle SQL Queries'''
    get_table_names = '''\
        SELECT TNAME FROM tab\
    '''
    get_column_name_type_length_where = '''\
        SELECT column_name, data_type, data_length
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table_name}'\
    '''

def run_unittests():
    '''Runs all unittests in the unittests.py file, and exits.'''
    import unittests # local file
    unittests.unittest.main()
    exit(0)


if __name__ == '__main__':
    main()
