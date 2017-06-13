#!/usr/bin/python
'''\
Sample implementation of the cx_Oracle library.
We only handle connection establishment, and return a raw connection/cursor
object.

Usage: {0} <table_name>

Note: 
    We will ask you for a database password if necessary.
    The default user account is {1}.\
'''

import utility
import cx_Oracle
import sys
from getpass import getpass
import os # environ -> db pw
from utility import OracleSQLQueries as OSQL

# Don't worry he said, it's all internal he said.
USR = 'yuca05'
H_KAPPA = 'kappa.ciat.cgiar.org'
H_RESEARCH = 'research.ciat.cgiar.org'
PORT = 1521
SID_KAPPA = 'CIAT'
SID_RESEARCH = 'CIAT'
SCHEMA = 'YUCA05'

class Oracledb():
    '''Simple wrapper around cx_Oracle .makedsn, .connect, and some more basic
    connection setup handling.'''

    debug = False

    def __init__(self, usr=USR, host=H_KAPPA, port=PORT, sid=SID_KAPPA, pw='',
                 schema=SCHEMA, dsn=None):
        '''Defaults to initialization with global variables.
        Note that if you provide a 'dsn', the 'host', 'port', and 'sid'
        argument will be discarded.'''
        self.USR = usr
        self.HOST = host
        self.PORT = port
        self.SID = sid
        self.__PW = pw
        self.SCHEMA = schema
        self.DSN = dsn
        self.con = None
        self.cur = None
        self.saved_curs = {}

    def connect(self):
        '''Returns a tuple(connection_obj, cursor_obj).'''
        # The following 3 if-statements MUST be exactly in this order.
        if not self.DSN:
            self.DSN = cx_Oracle.makedsn(self.HOST, self.PORT, self.SID)
        if self.debug:
            print '[+] connecting ( usr=%s, pw=1234, dsn=%s )' % (self.USR,
                                                                  self.DSN)
        if not self.__PW:
            if os.environ.has_key('ORACLEDB_PW'):
                self.__PW = os.environ['ORACLEDB_PW']
            else:
                self.__PW = getpass(prompt='Oracledb Password: ')
        if not self.con:
            self.con = cx_Oracle.connect(self.USR, self.__PW, self.DSN)
            self.con.current_schema = self.SCHEMA
        if not self.cur:
            self.cur = self.con.cursor()
        return self.con, self.cur

    def get_rows(self, sql, table=None, fetchamount=None, raw=False, save_as=None):
        '''Execute a <sql>-statement, returns a namedtuple.
        
        If <table> is not given we return the raw data, else it is used to
        fetch the column headers to create the namedtuples.
        If <fetchamount> is given, only that amount is fetched and returned.
        '''
        self.cur.execute(sql.format(table=table))
        if fetchamount:
            data = self.cur.fetchmany(fetchamount)
        else:
            data = self.cur.fetchall()

        if table and not raw:
            self.lasttable = table
            self.lastraw = raw
            header_sql = OSQL.get_column_metadata_from.format(table=table)
            self.cur.execute(header_sql)
            headers = [utility.normalize(i[1]) for i in self.cur.fetchall()]
            self.lastheaders = headers
            data = utility.make_namedtuple_with_headers(headers, table, data)
        else:
            print '[get_rows] need table=<> to return non-raw data'

        if save_as:
            self.saved_curs.update({save_as : self.cur})
            self.cur = self.con.cursor()

        return data

    def fetch_more(self, n=None, raw=False, table=None, from_saved=None):
        '''Fetch more result from the last query, remembering the last output
        format.'''
        if from_saved:
            c = self.saved_curs[from_saved]
            print 'ccc, got saved cursor called:', from_saved
        else:
            c = self.cur
        if not n:
            data = c.fetchall()
        else:
            data = c.fetchmany(n)
            print 'ccc, fetched manny:', n
        if len(data) == 0:
            print 'VVV, nothing in here:', data
            return []

        try:
            raw = self.lastraw
        except AttributeError:
            raw = False

        if raw:
            return data
        elif not table:
            table = self.lasttable

        self.lasttable = table
        self.lastraw = raw
        if hasattr(self, 'lastheaders'):
            return utility.make_namedtuple_with_headers(self.lastheaders,
                                                        table, data)
        else:
            header_sql = OSQL.get_column_metadata_from.format(table=table)
            return utility.make_namedtuple_with_query(self.con.cursor(),
                                                      header_sql, table, data)

def main():
    '''<nodoc>'''
    if len(sys.argv) < 2:
        usage()
        exit(1)
    table = sys.argv[1]

    Oracledb.debug = True
    db = Oracledb()
    con, c = db.connect()
    sql = '''SELECT * FROM {}'''.format(table)
    try:
        print '[+] executing : "{}"'.format(sql)
        c.execute(sql)
        con.commit()
        print '[+] Exito, mirame imprimido row[:15] -> '
        # Print nicely formatted example values.
        for name, value in zip(c.description[:16], c.fetchone()[:16]):
            print '{0:18} : {1}'.format( name[0], value )
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print >> sys.stderr, '[f] code: %d\n[f] msg: %s' % (error.code,
                                                            error.message)
        return 1
    return 0

def usage():
    '''Prints this file's usage as table dumping test tool.'''
    print __doc__.format(sys.argv[0], USR)

def gimme_con():
    '''Returns a connection object to the database with all the default
    arguments.'''
    db = Oracledb()
    return db.connect()[0]

if __name__ == '__main__':
    main()
