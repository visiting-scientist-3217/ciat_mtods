#!/usr/bin/python
'''\
MtODS -- _M_igrate _t_he _O_racle _D_atabase _S_tuff
To our Chado schema, by .. creating excel files, which are then parsed and
uploaded by MCL.

NOTE For production we should rewrite the 'Full Migration'-option, to start
threads executing the 'Single Table'-option, as locking all the objects
passed to the TableGuru might be more work and not necessarily faster.
'''

# Official libraries.
import ConfigParser # Reading the table translation file.
import optparse
import os
import sys

# Local stuff.
import drush
import table_guru
import migration

# Default values.
BASE_DIR = os.getcwd()
CONF_FILENAME = 'trans.conf'
table_guru.CONF_PATH = BASE_DIR + CONF_FILENAME

def main():
    '''Read the __doc__ string of this module.'''
    parser = optparse_init()
    o, args = parser.parse_args()

    # Needed for unittests to run their main().
    sys.argv = [''] 

    if args:
        print 'Whts dis? -> "%s" ??' % (args)
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

    if o.drush_root:
        drush.Drush.DRUPAL_PATH = o.drush_root
    if o.basedir:
        drush.Drush.BASE_DIR = o.basedir
        migration.Migration.BASE_DIR = o.basedir
    if o.config_path:
        table_guru.CONF_PATH = o.config_path
    drush.test_and_configure_drush()

    if o.only_unittests:
        run_unittests()

    # Here we go..
    migra = migration.Migration(verbose=o.verbose, quiet=o.quiet)

    if o.do_update:
        migra.update(basedir=o.basedir)
    elif o.basedir or not o.single_table:
        migra.full(basedir=o.basedir, upload=o.do_upload)
    else:
        migra.single(o.single_table)

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
    p.add_option('-c', '--config', action='store', type='string',
        dest='config_path', help=\
        'path to the table translation config (default: {})'\
        .format('<basedir>/'+CONF_FILENAME), metavar='<path>', default='')
    p.add_option('-u', '--update', action='store_true', dest='do_update',
        help='update: only get new data from Oracle', metavar='',
        default=False)

    test = optparse.OptionGroup(p, 'Test Options', 'Some Testting utility')
    test.add_option('-t', '--test', action='store_true', dest='only_unittests',
        help='only run testCases and exit', metavar='', default=False)
    test.add_option('-n', '--no-upload', action='store_false', dest='do_upload',
        help='do NOT upload the spreadsheed via drush (default: False)',
        metavar='', default=True)

    # Full migration options
    pg_all = optparse.OptionGroup(p, 'Full Migration', 'Options for a full'+\
        ' migration of all known tables to the Chado database.\nThis is '+\
        'the default operation.')
    pg_all.add_option('-b', '--basedir', action='store', type='string',
        dest='basedir', help='default to {}'.format(BASE_DIR),
        metavar='<path>', default=BASE_DIR)

    # Single table options
    pg_single = optparse.OptionGroup(p, 'Single Table Options', 'Only use a'+\
        ' single specified table instead of all implemented ones.\nThis '+\
        'gets enabled by supplying the [-s] option.')
    pg_single.add_option('-s', '--singletab', action='store', type='string',
        dest='single_table', help='specify only a single table to migrate',
        metavar='<table>', default='')

    p.add_option_group(pg_all)
    p.add_option_group(pg_single)
    p.add_option_group(test)
    return p

def run_unittests():
    '''Runs all unittests in the unittests.py file, and exits.'''
    import unittests # local file
    unittests.unittest.main()
    exit(0)


if __name__ == '__main__':
    main()
