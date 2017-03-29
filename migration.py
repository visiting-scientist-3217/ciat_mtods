'''The Migration-Task-Class'''
import utility
import os
import drush
import table_guru

# Note: this v is not the official cx_Oracle library, but a convenient wrapper.
import cx_oracle


class Migration(utility.VerboseQuiet):
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

    BASE_DIR = ''

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
                self.single(table)

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
        tg = table_guru.TableGuru(table, self.cursor, self.VERBOSE)
        names = tg.create_workbooks()
        for name in names:
            self.xlsx_files += name

    def upload(self, filename):
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
        self.cursor.execute(utility.OracleSQLQueries.get_table_names)
        table_names = self.cursor.fetchall()
        if not table_names:
            raise RuntimeError('[.get_tables] failed to fetch table names')
        table_names = [t[0] for t in table_names]
        return table_names

