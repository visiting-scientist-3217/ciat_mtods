'''The Migration-Task-Class'''
import utility # must be first cause of monkey-patching
import os
import table_guru
import threading
from utility import Task

# Note: this v is not the official cx_Oracle library, but a convenient wrapper.
import cx_oracle


class Migration(utility.VerboseQuiet):
    '''Handler of the migration/translation task.

    The attribute .only_update decides if we get all data, or only the new data
    from the Oracle database.

    Tasks are selected by following methods:
        .full()             migrate all known tables
            basedir=<default:$PWD>
        .single(<table>)    migrate <table>
    '''

    TABLES_MIGRATION_IMPLEMENTED = [
        'VM_RESUMEN_EVAL_AVANZADAS',
    ]
    TABLES_MIGRATION_NOT_IMPLEMENTED = [
        'VM_RESUMEN_ENFERMEDADES',
        'VM_RESUMEN_EVAL_CALIDAD',
        'VM_RESUMEN_EVAL_MOSCA_BLANCA',
    ]

    # Default Chado DB and CV names.
    DB_NAME = 'mcl_pheno'
    CV_NAME = 'mcl_pheno'

    BASE_DIR = ''

    def __init__(self, verbose=False, quiet=False, basedir=None, **tgargs):
        '''We set some configuration, connect to the database, and create a
        local cursor object.

        Arguments:
            verbose     print lots of debug info
            quiet       daemon mode, be silent
        '''
        super(self.__class__, self).__init__()
        if not basedir:
            if self.BASE_DIR: basedir = self.BASE_DIR
            else:             basedir = os.getcwd()
        else: self.BASE_DIR = basedir
        self.VERBOSE = verbose
        self.QUIET = quiet
        self.db = cx_oracle.Oracledb()
        if self.VERBOSE: self.db.debug = True
        self.connection, self.cursor = self.db.connect()
        self.vprint('[+] connected')
        self.tg = table_guru.TableGuru('', self.db, self.VERBOSE,
                                       basedir=basedir, **tgargs)

    def __get_tables(self):
        self.cursor.execute(utility.OracleSQLQueries.get_table_names)
        table_names = self.cursor.fetchall()
        if not table_names:
            raise RuntimeError('[.__get_tables] failed to fetch table names')
        table_names = [t[0] for t in table_names]
        return table_names

    def full(self, basedir=BASE_DIR):
        '''We call the table migration task for all tables in
        TABLES_MIGRATION_IMPLEMENTED.

        Arguments:
            basedir     log file location
        '''
        if not os.path.exists(basedir):
            msg = '[.full] non existent path "{}"'
            raise RuntimeError(msg.format(basedir))
        self.vprint('[+] basedir = "{0}"'.format(basedir))

        for table in self.__get_tables():
            if table in self.TABLES_MIGRATION_IMPLEMENTED:
                self.single(table)

    def single(self, table):
        '''Migrates a single table, including upload if specified.'''
        self.vprint('[+] starting migrate({})'.format(table))
        self.tg.table = table
        tasks_generator = self.tg.create_upload_tasks()
        for suite in tasks_generator:
            Task.parallel_upload(suite)

