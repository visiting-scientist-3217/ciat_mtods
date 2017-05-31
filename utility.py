'''Utility includes:
    - base classes
    - SQL Querie namespaces
    - commonly used functions
    - ?
'''
import threading
from collections import namedtuple
from re import sub

def invert_dict(d):
    '''Switch keys with values in a dict.'''
    new = {}
    for k,v in d.iteritems():
        new.update({v:k})
    return new

def make_namedtuple_with_query(cursor, query, name, data):
    '''Return <data> as a named tuple, called <name>.
    
    Create a named tuple called <name> using <query> and <cursor> to retreive
    the headers. Then format data with the created tuple and return it.

    Assumptions:
     - The result of query yields a list/tuple of results, which contain the
       desired name at position [1], e.g.:
        [(SomeThing, 'col_name1'), (SomeThing, 'col_name2'), ...]
     - Data must be iterable
    '''
    cursor.execute(query)
    headers = [normalize(i[1]) for i in cursor.fetchall()]
    NTuple = namedtuple(name, headers)
    result = [NTuple(*r) for r in data]
    return result

def normalize(s):
    '''Some string substitutions, to make it a valid Attribute.

    (Not complete)
    '''
    ss = sub(r'\s+', '_', s)
    return ss

class OracleSQLQueries():
    '''Namespace for format()-able Oracle SQL Queries'''
    get_table_names = '''\
        SELECT TNAME FROM tab\
    '''
    get_column_metadata_from = '''\
        SELECT table_name, column_name, data_type, data_length, COLUMN_ID
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table}'
            ORDER BY COLUMN_ID\
    '''
    get_all_from = '''\
        SELECT * FROM {table}\
    '''

class PostgreSQLQueries():
    '''Namespace for format()-able PostgreSQL queries.'''
    select_all = '''\
        SELECT * FROM {table}\
    '''
    select_all_from_where_eq = '''\
        SELECT * FROM {table} WHERE {col} = '{name}'\
    '''
    select_count = '''\
        SELECT count(*) FROM {table}\
    '''
    insert_into_table = '''\
        INSERT INTO {table} {columns} VALUES {values}\
    '''
    delete_where = '''\
        DELETE FROM {table} WHERE {cond}\
    '''
    column_names = '''\
        SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_name='{table}'\
    '''


class VerboseQuiet(object):
    '''To be inherited from.'''
    def __init__(self):
        self.printlock = threading.Lock()
        self.VERBOSE = False
        self.QUIET = False
    def vprint(self, s):
        '''Only print stuff, if we are in verbose mode.'''
        if hasattr(self, 'printlock'):
            self.printlock.acquire()
        if self.VERBOSE:
            print s
        if hasattr(self, 'printlock'):
            self.printlock.release()

    def qprint(self, s):
        '''Only print stuff, if we are NOT in quiet mode.'''
        if hasattr(self, 'printlock'):
            self.printlock.acquire()
        if not self.QUIET:
            print s
        if hasattr(self, 'printlock'):
            self.printlock.release()

class Task(VerboseQuiet):
    '''Used for multithreading implementation.'''
    def __init__(self, name, job, *args, **kwargs):
        super(self.__class__, self).__init__()
        #self.VERBOSE = True
        self.name = name
        self.job = job
        self.args = args
        self.kwargs = kwargs
    def execute(self):
        msg = '{}.execute()'
        self.vprint(msg.format(self.__str__()[:40]+'...)'))
        self.job(*self.args, **self.kwargs)
    def __str__(self):
        s = 'Task(name={name}, job={job}, default_args={dargs},'\
            + ' args={args}, kwargs={kwargs}'
        s = s.format(name=self.name, job=self.job.func_name,
                     dargs=self.job.func_defaults, args=self.args,
                     kwargs=self.kwargs)
        return s
    def __repr__(self):
        return self.__str__()

