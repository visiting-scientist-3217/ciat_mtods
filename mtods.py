#!/work/opt/python-2.7.9/bin/python
'''\
MtODS -- _M_igrate _t_he _O_racle _D_atabase _S_tuff
 .. to our Chado schema.
'''

# global
import ConfigParser # Reading the table translation file.
import optparse
import os
import sys

# local
import table_guru
import migration
import chado

BASE_DIR = os.getcwd()
CONF_FILENAME = 'trans.conf'
table_guru.CONF_PATH = os.path.join(BASE_DIR, CONF_FILENAME)

def main():
    '''Read the __doc__ string of this module.'''
    parser = optparse_init()
    o, args = parser.parse_args()

    if args:
        print 'Unknown argument: "{}"'.format(args)
        exit(1)

    mutally_exclusive_msg = 'options {0} and {1} are mutally exclusive'
    exclusive_opts = zip(
        [[o.verbose,'-v'],], 
        [[o.quiet,  '-q'],]
    )
    for a,b in exclusive_opts:
        if a[0] and b[0]:
            parser.error(mutally_exclusive_msg.format(a[1],b[1]))

    if o.config_path:
        table_guru.CONF_PATH = o.config_path

    if o.pg_user:
        chado.USER = o.pg_user
    if o.pg_db:
        chado.DB = o.pg_db
    chado.HOST = o.pg_host
    chado.PORT = o.pg_port

    migra = migration.Migration(verbose=o.verbose, quiet=o.quiet,
                                chado_db=o.ch_db, chado_cv=o.ch_cv,
                                chado_dataset=o.ch_pj)
    if o.single_table:
        migra.single(o.single_table)
    else:
        migra.full()

    return 0

def optparse_init():
    '''Return an initialized optparse.OptionParser object, ready to parse the
    command line.
    '''
    p = optparse.OptionParser(description=__doc__)
    p.add_option('-v', '--verbose', action='store_true', dest='verbose',
        help='verbose', metavar='', default=False)
    p.add_option('-q', '--quiet', action='store_true', dest='quiet',
        help='only print critical failure information', metavar='',
        default=False)
    p.add_option('-c', '--config', action='store', type='string',
        dest='config_path', help=\
        'path to the table translation config (default: {})'\
        .format('<basedir>/'+CONF_FILENAME), metavar='<path>', default='')

    pgo = optparse.OptionGroup(p, 'Postgres Connection Options', '')
    pgo.add_option('-y', '--pg_host', action='store', type='string',
        dest='pg_host', help='postgres hostname, defaults to localhost',
        metavar='<host>', default=None)
    pgo.add_option('-p', '--pg_port', action='store', type='string',
        dest='pg_port', help='postgres port, defaults to 5432', metavar='N',
        default=None)
    pgo.add_option('-t', '--pg_user', action='store', type='string',
        dest='pg_user', help='postgres user, defaults to the current user',
        metavar='<user>')
    pgo.add_option('-d', '--pg_db', action='store', type='string',
        dest='pg_db', help='postgres db, defaults to drupal7', metavar='<db>')

    pch = optparse.OptionGroup(p, 'Chado/Drupal7 Config Options', 
        'We require these entries to already be in the database.')
    pch.add_option('--ch-db', action='store', type='string', dest='ch_db',
        help='db entry used for phenotyping data (default: mcl_pheno)',
        metavar='<db>', default='mcl_pheno')
    pch.add_option('--ch-cv', action='store', type='string', dest='ch_cv',
        help='cv entry used for phenotyping data (default: mcl_pheno)',
        metavar='<cv>', default='mcl_pheno')
    pch.add_option('--ch-pj', action='store', type='string', dest='ch_pj',
        help='dataset entry used for phenotyping data (default: mcl_pheno)',
        metavar='<pj>', default='mcl_pheno')

    single = optparse.OptionGroup(p, 'Single Table Options',
        'By default we get data from all tables, for which a translation is'\
        ' possible. Specifying this option only given table is used.')
    single.add_option('-s', '--singletab', action='store', type='string',
        dest='single_table', help='specify a OracleDB table to migrate',
        metavar='<table>', default='')

    p.add_option_group(pgo)
    p.add_option_group(pch)
    p.add_option_group(single)
    return p

if __name__ == '__main__':
    main()
