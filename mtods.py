#!/usr/bin/python
'''\
MtODS -- _M_igrate _t_he _O_racle _D_atabase _S_tuff
 .. to our Chado schema.
'''

#NOTE For production we should rewrite the 'Full Migration'-option, to start
# threads executing the 'Single Table'-option, as locking all the objects
# passed to the TableGuru might be more work and not necessarily faster.

# Official libraries.
import ConfigParser # Reading the table translation file.
import optparse
import os
import sys

# Local stuff.
import drush
import table_guru
import migration
import chado

# Default values.
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

    # Check user input for crap.
    mutally_exclusive_msg = 'options {0} and {1} are mutally exclusive'
    exclusive_opts = zip(
        [[o.single_table,'-s'], [o.verbose,'-v'], [o.do_update,    '-u']], 
        [[o.basedir,     '-b'], [o.quiet,  '-q'], [not o.do_upload,'-n']]
    )
    for a,b in exclusive_opts:
        if a[0] and b[0]:
            parser.error(mutally_exclusive_msg.format(a[1],b[1]))

    drush.DEFAULT_USR = o.drupal_usr
    if o.drush_root:
        drush.Drush.DRUPAL_PATH = o.drush_root
    if o.basedir:
        drush.Drush.BASE_DIR = o.basedir
        migration.Migration.BASE_DIR = o.basedir
    if o.config_path:
        table_guru.CONF_PATH = o.config_path

    if o.pg_user:
        chado.USER = o.pg_user
    if o.pg_db:
        chado.DB = o.pg_db
    chado.HOST = o.pg_host
    chado.PORT = o.pg_port

    # Here we go..
    migra = migration.Migration(verbose=o.verbose, quiet=o.quiet)

    if o.basedir or not o.single_table:
        migra.full(basedir=o.basedir, upload=o.do_upload,
                   only_update=o.do_update, chado_db=o.ch_db, chado_cv=o.ch_cv,
                   chado_dataset=o.ch_pj)
    else:
        migra.single(o.single_table, chado_db=o.ch_db, chado_cv=o.ch_cv,
                     chado_dataset=o.ch_pj)
    return 0

def optparse_init():
    '''Return an initialized optparse.OptionParser object, ready to parse the
    command line.
    '''
    p = optparse.OptionParser(description=__doc__)
    p.add_option('-v', '--verbose', action='store_true', dest='verbose',
        help='hablamelo', metavar='', default=False)
    p.add_option('-q', '--quiet', action='store_true', dest='quiet',
        help='daemon silence', metavar='', default=False)
    p.add_option('-r', '--drush-root', action='store', type='string',
        dest='drush_root', help='root dir of drupal installation',
        metavar='<path>', default='')
    p.add_option('--drupal-user', action='store', type='string',
        dest='drupal_usr', help='drupal user, must be able to execute MCL drush'\
        +'commands', metavar='<usr>', default='admin')
    p.add_option('-c', '--config', action='store', type='string',
        dest='config_path', help=\
        'path to the table translation config (default: {})'\
        .format('<basedir>/'+CONF_FILENAME), metavar='<path>', default='')
    p.add_option('-u', '--update', action='store_true', dest='do_update',
        help='update: only get new data from Oracle', metavar='',
        default=False)

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

    pch = optparse.OptionGroup(p, 'Chado/Drupal7 Config Options', 'MCL needs a'\
        + ' specific setup of project, projectprop and cv to be able to upload'\
        + ' phenotyping data..')
    pch.add_option('--ch-db', action='store', type='string', dest='ch_db',
        help='that db entry (default: mcl_pheno)', metavar='<db>',
        default='mcl_pheno')
    pch.add_option('--ch-cv', action='store', type='string', dest='ch_cv',
        help='that cv entry (default: mcl_pheno)', metavar='<cv>',
        default='mcl_pheno')
    pch.add_option('--ch-pj', action='store', type='string', dest='ch_pj',
        help='that pj entry (default: mcl_pheno)', metavar='<pj>',
        default='mcl_pheno')

    test = optparse.OptionGroup(p, 'Debug Options', '')
    test.add_option('-n', '--no-upload', action='store_false', dest='do_upload',
        help='do NOT upload the spreadsheed via drush (default: False)',
        metavar='', default=True)

    # Full migration options
    full = optparse.OptionGroup(p, 'Full Migration (default)', '')
    full.add_option('-b', '--basedir', action='store', type='string',
        dest='basedir', help='defaults to {}'.format(BASE_DIR),
        metavar='<path>', default=BASE_DIR)

    # Single table options
    single = optparse.OptionGroup(p, 'Single Table Options', 'Only use a'+\
     ' single specified table instead of all implemented ones.')
    single.add_option('-s', '--singletab', action='store', type='string',
        dest='single_table', help='specify only a single table to migrate',
        metavar='<table>', default='')

    p.add_option_group(pgo)
    p.add_option_group(pch)
    p.add_option_group(full)
    p.add_option_group(single)
    p.add_option_group(test)
    return p

if __name__ == '__main__':
    main()
