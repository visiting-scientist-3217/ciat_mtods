'''The Migration-Task-Class'''
import utility
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

    # List of Tables, for which a Chado migration is implemented.
    TABLES_MIGRATION_IMPLEMENTED = [
        'VM_RESUMEN_EVAL_AVANZADAS', # this one first..
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

    def __init__(self, upload=True, verbose=False, quiet=False,
                 only_update=False, **tgargs):
        '''We set some configuration, connect to the database, and create a
        local cursor object.

        Arguments:
            verbose     print lots of debug info
            quiet       daemon mode, be silent
            upload      create AND upload the excel files, default: true

        Writable members:
            do_upload   intended for unittesting
        '''
        super(self.__class__, self).__init__()
        self.VERBOSE = verbose
        self.QUIET = quiet
        self.do_upload = upload
        self.only_update = only_update
        self.db = cx_oracle.Oracledb()
        if self.VERBOSE: self.db.debug = True

        self.connection, self.cursor = self.db.connect()
        self.vprint('[+] connected')

        self.tg = table_guru.TableGuru('', self.db, self.VERBOSE,
                                       basedir=self.basedir,
                                       update=self.only_update, **tgargs)

    def __get_tables(self):
        self.cursor.execute(utility.OracleSQLQueries.get_table_names)
        table_names = self.cursor.fetchall()
        if not table_names:
            raise RuntimeError('[.__get_tables] failed to fetch table names')
        table_names = [t[0] for t in table_names]
        return table_names

    def __parallel_upload(self, tasks):
        '''Start upload instances in as manny threads as possible.

        Syntax: A tuple() declares ordered execution, a list() parallel
                execution.

        Examples for tasks = ..
            [a, b, c]
                a, b and c will be uploaded in parallelly
            [(a, b), c]
                b will be uploaded after a
                and (a,b) will be uploaded parallelly to c
            ([a, b], c)
                a and b will be uploaded parallelly
                and [a,b] will be uploaded before c

        Realistically we can only parallelize stocks and sites and contacts.
            => ([stocks, sites], phenotypes)
        '''
        if type(tasks) == tuple:
            for t in tasks:
                self.__parallel_upload(t)
        elif type(tasks) == list:
            ts = []
            for t in tasks:
                t = threading.Thread(target=self.__parallel_upload, args=(t))
                ts.append(t)
            map(lambda x: x.start(), ts)
            map(lambda x: x.join(), ts)
        else:
            self.vprint('[+] starting upload: {}'.format(tasks))
            tasks.execute()

    def __non_parallel_upload(self, tasks):
        '''Non parallel upload for testing purposes.'''
        if not tasks is Task:
            for t in tasks:
                self.__non_parallel_upload(t)
        else:
            tasks.execute()

    def full(self, basedir=BASE_DIR):
        '''We call the table migration task for all tables in
        TABLES_MIGRATION_IMPLEMENTED.

        Arguments:
            basedir     log file location
        '''
        if not os.path.exists(basedir):
            raise RuntimeError('[.full] non existent path "{}"'\
                               .format(basedir))
        self.basedir = basedir
        self.vprint('[+] basedir = "{0}", do_upload = {1}'.format(self.basedir,
            self.do_upload))

        for table in self.__get_tables():
            if table in self.TABLES_MIGRATION_IMPLEMENTED:
                self.single(table)

    def single(self, table):
        '''Migrates a single table, including upload if specified.'''
        self.vprint('[+] starting migrate({})'.format(table))
        self.tg.table = table
        tasks = self.tg.create_upload_tasks(update=self.only_update)
        for t in tasks:
            self.__parallel_upload(t)

