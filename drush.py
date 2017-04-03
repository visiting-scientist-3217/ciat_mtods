''' '''
import os
import time
# Calling drush, we want status and ouput
try:
    from commands import getstatusoutput
except ImportError:
    from subprocess import getstatusoutput

class Drush():
    '''Wrapper around the shell programm 'drush'.

    Contains default config, MainlabChadoLoader commands (mcl).

    Note: Needs to be configured manually for the current drupal AND drush
          installation.
          Plus you should call test_and_configure_drush().
    '''
    BINARY = '/usr/bin/drush'
    TEMPLATE = '{sudo} {binary} --root={root} {cmd} {args}'
    DRUPAL_PATH = '/usr/share/drupal7'
    MCL_UPLOAD = 'mcl-submit-uploading'   # args: ?
    MCL_LIST = 'mcl-list-jobs'            # args: <username>
    MCL_INFO = 'mcl-job-info'             # args: <job-id>
    MCL_DEL_JOB = 'mcl-delete-job'        # args: <job-id>
    NEED_SUDO = False

    BASE_DIR = '' # see migrate_the_ora....

    @staticmethod
    def execute(command, arguments, nodump=False, quiet=False):
        '''Creates a drush command line, executes it and returns a
        tuple(status, output).
        '''
        sudo = ''
        if Drush.NEED_SUDO:
            sudo = 'sudo'
        drush_cmd = Drush.TEMPLATE.format(sudo=sudo, binary=Drush.BINARY,
            root=Drush.DRUPAL_PATH, cmd=command, args=arguments)
        if Drush.NEED_SUDO:
            print '[!] ' + drush_cmd
        status, out = getstatusoutput(drush_cmd)
        if not out:
            return status, None
        if not nodump and '[error]' in out:
            dump_output('drush_{}.log'.format(command), out, quiet)
        return status, out

def test_and_configure():
    '''Execute a sample drush comandline and checks the output.
    
    First execution is silent. If we fail, either config is wrong or we need
    more priviledges, so we try again with sudo. If we still fail, construct a
    nice error message, and dump the output.
    '''
    status, out = Drush.execute('pml', '', nodump=True, quiet=True)
    if status == 0:
        return
    else:
        Drush.NEED_SUDO = True
        status, out = Drush.execute('pml', '')
        if status != 0:
            dump_output('drush_fail.log', out)
            msg = '[-] drush exited with {0}'.format(status)
            raise RuntimeError(msg)

def dump_output(name, dump, q=False):
    '''Create a dumpfile named <name>, located at <Drush.BASE_DIR>. If the file
    exists, we prefix the current date and time.
    '''
    dumpfile_template = '{path}/{opt}{file}'
    dumpfile = dumpfile_template.format(path=Drush.BASE_DIR, opt='', file=name)
    if os.path.exists(dumpfile):
        now = time.strftime('%m_%d_%H-%M-%S_', time.localtime())
        dumpfile = dumpfile_template.format(path=Drush.BASE_DIR, opt=now,
            file=name)
    with open(dumpfile, 'w') as fd:
        fd.write(dump)
    if not q:
        print '[-] dump written "{}"'.format(dumpfile)

