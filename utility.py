'''Utility includes:
    - base classes
    - SQL Querie namespaces
    - commonly used functions
    - ?
'''
import threading
from collections import namedtuple
from re import sub

# So psycopg2 is level 2 threadsave, but our std-lib is not. Here you see the
# easiest way I could think of to fix this:
from gevent import monkey
monkey.patch_all()

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
    def __acq(self):
        if hasattr(self, 'printlock'):
            self.printlock.acquire()
    def __rel(self):
        if hasattr(self, 'printlock'):
            self.printlock.release()
    def vprint(self, s):
        '''Only print stuff, if we are in verbose mode.'''
        if self.VERBOSE:
            self.__acq()
            print s
            self.__rel()

    def qprint(self, s):
        '''Only print stuff, if we are NOT in quiet mode.'''
        if not self.QUIET:
            self.__acq()
            print s
            self.__rel()

class Task(VerboseQuiet):
    '''Used for multithreading implementation.'''
    def __init__(self, name, job, *args, **kwargs):
        super(self.__class__, self).__init__()
        self.VERBOSE = True
        self.name = name
        self.job = job
        self.args = args
        self.kwargs = kwargs
    def execute(self, thread=False):
        '''Calls self.job with given args and kwargs.'''
        if thread:
            msg = '\n[exec-thrd] {}'
        else:
            msg = '\n[exec] {}'
        self.vprint(msg.format(self.__str__()[:70]+'...)'))
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

    @staticmethod
    def init_empty():
        '''Does nothing when executed.'''
        return Task('Empty', lambda args,kwargs: None, [], {})

    @staticmethod
    def non_parallel_upload(tasks):
        '''Non parallel upload for testing purposes.'''
        if not type(tasks) == Task:
            for t in tasks:
                Task.non_parallel_upload(t)
        else:
            tasks.execute()

    @staticmethod
    def parallel_upload(tasks):
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
            => ([stocks, sites, ?], phenotypes)
        '''
        if type(tasks) == tuple:
            for t in tasks:
                Task.parallel_upload(t)
        elif type(tasks) == list:
            ts = []
            for t in tasks:
                t = threading.Thread(target=Task.parallel_upload, args=[t])
                ts.append(t)
            map(lambda x: x.start(), ts)
            map(lambda x: x.join(), ts)
        else:
            tasks.execute(thread=True)

    @staticmethod
    def print_tasks(ts, pre=''):
        '''One level of indentation equals parallel execution.'''
        if type(ts) == list:
            for t in ts:
                Task.print_tasks(t, pre=pre)
        elif type(ts) == tuple:
            for t in ts:
                Task.print_tasks(t, pre=pre+'\t')
        else:
            if pre != '': pre = pre + '>'
            print pre, str(ts)[:50]+'...'
