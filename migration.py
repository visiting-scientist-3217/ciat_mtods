'''The Migration-Task-Class'''
import utility
import os
import drush
import table_guru
import threading

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
            do_upload=<default:True>
            filename=<default:test.xlsx>
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
        self.VERBOSE = verbose
        self.QUIET = quiet
        self.do_upload = upload
        self.xlsx_files = []
        self.only_update = only_update
        self.drush = drush.Drush()

        # NOTE We only need the db connection + cursor in this class to know
        # all table names, and we pass that cursor on to the TableGuru, to
        # avoid making another db connection.
        self.db = cx_oracle.Oracledb()
        if self.VERBOSE: self.db.debug = True
        self.connection, self.cursor = self.db.connect()
        self.vprint('[+] connected')

    def full(self, basedir=BASE_DIR):
        '''We call the table migration task for all tables in
        TABLES_MIGRATION_IMPLEMENTED.

        Arguments:
            basedir     excel file storage location
        '''
        if not os.path.exists(basedir):
            raise RuntimeError('[.full] non existent path "{}"'\
                               .format(basedir))
        self.basedir = basedir
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
            for f in self.xlsx_files:
                self.upload(f)
            #self.__parallel_upload(self.xlsx_files) # TODO enable me

    def __parallel_upload(self, files):
        '''Start drush-mcl-upload instances in as manny threads as possible.

        Note: A tuple() declares ordered execution, a list() parallel
              execution.

        Examples for files = ..
            [a, b, c]
                a, b and c will be uploaded in parallelly
            [(a, b), c]
                b will be uploaded after a
                and [a,b] will be uploaded parallelly to c
            ([a, b], c)
                a and b will be uploaded parallelly
                and [a,b] will be uploaded before c

        Realistically we can parallelize
            - all 'pre_*' spreadsheets
            - stocks and sites
                => ([pre1, pre2, ..], [stocks, sites], phenotypes)
        '''
        if type(files) == tuple:
            for f in files:
                self.__parallel_upload(f)
        elif type(files) == list:
            ts = []
            for f in files:
                t = threading.Thread(target=self.__parallel_upload, args=(f))
                ts.append(t)
            map(lambda x: x.start(), ts)
            map(lambda x: x.join(), ts)
        else:
            self.upload(f)

    def create_xlsx_from(self, table):
        '''Does excactly that.

        Note: We might create multiple worksheets, even when only migrating a
              single table. These MUST be uploaded in the given order.
        '''
        tg = table_guru.TableGuru(table, self.db, self.VERBOSE,
                                  basedir=self.basedir,
                                  update=self.only_update, **tgargs)
        names = tg.create_workbooks(update=self.only_update)
        if self.xlsx_files:
            self.vprint('[.create_xlsx_from] clearing self.xlsx_files')
            self.xlsx_files = []
        for name in names:
            self.xlsx_files.append(name)

    def upload(self, fname):
        '''Upload the given xlsx file.

        We upload by calling `drush` with some args.

        Note: We SHOULD bypass drush + MCL + Excel all along, and just put all
              mannually into the Chado tables.
        '''
        if fname[-4:] != 'xlsx':
            self.qprint('[.upload] fname does not end in xlsx')
        if not fname[0] == os.path.sep:
            fname = os.path.join(os.getcwd(), fname)

        status, out = self.drush.mcl_upload(fname)

        if status != 0:
            qprint('[.upload] drush failed with: {0} {1}',
                self.drush.MCL_UPLOAD.format(file=fname))
        else:
            msg = '[+] drush cmd successfull:\n{}'
            self.vprint(msg.format('[...] '+out[-100:]))
        return (status, out, fname)

    def get_tables(self):
        self.cursor.execute(utility.OracleSQLQueries.get_table_names)
        table_names = self.cursor.fetchall()
        if not table_names:
            raise RuntimeError('[.get_tables] failed to fetch table names')
        table_names = [t[0] for t in table_names]
        return table_names

