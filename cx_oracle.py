#!/usr/bin/python
'''\
Sample implementation of the cx_Oracle library.

Usage: {0} <table_name>

Note: 
    We will ask you for a database password if necessary.
    The default user account is {1}.\
'''

import cx_Oracle
import sys
from getpass import getpass

# Don't worry he said, it's all internal.
USR = 'yuca05'
H_KAPPA = 'kappa.ciat.cgiar.org'
H_RESEARCH = 'research.ciat.cgiar.org'
PORT = 1521
SID_KAPPA = 'CIAT'
SID_RESEARCH = 'CIAT'
PW = ''
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
        self.PW = pw
        self.SCHEMA = schema
        self.DSN = dsn

    def connect(self):
        '''Returns a tuple(connection_obj, cursor_obj).'''
        # Yes the following 3 if-statements are in the exact right order.
        if not self.DSN:
            self.DSN = cx_Oracle.makedsn(self.HOST, self.PORT, self.SID)
        if self.debug:
            print '[+] connecting ( usr=%s, pw=1234, dsn=%s )' % (self.USR,
                                                                  self.DSN)
        if not self.PW:
            self.PW = getpass()
        con = cx_Oracle.connect(self.USR, self.PW, self.DSN)
        con.current_schema = self.SCHEMA
        cur = con.cursor()
        return con, cur

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
