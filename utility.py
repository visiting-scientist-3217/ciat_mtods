'''Utility:
    - base classes
    - SQL Querie namespace
'''

class OracleSQLQueries():
    '''Namespace for Oracle SQL Queries'''
    get_table_names = '''\
        SELECT TNAME FROM tab\
    '''
    get_column_metadata_from = '''\
        SELECT table_name, column_name, data_type, data_length
            FROM USER_TAB_COLUMNS
            WHERE table_name = '{table_name}'\
    '''
    get_all_from = '''\
        SELECT * FROM {table}\
    '''

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

