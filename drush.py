''' '''
import os
import time
#import re
try: # Calling drush, we want status and ouput
    from commands import getstatusoutput
except ImportError:
    from subprocess import getstatusoutput

class Drush():
    '''Wrapper around the shell programm 'drush'.

    Contains default config, MainlabChadoLoader commands (mcl).

    Note: Needs to be configured manually for the current drupal AND drush
          installation.
    '''
    BINARY = '/usr/bin/drush'
    TEMPLATE = '{sudo} {binary} --root={root} {cmd} {args}'
    DRUPAL_PATH = '/usr/share/drupal7'
    MCL_UPLOAD = 'mcl-submit-uploading'   # args: ?
    MCL_LIST = 'mcl-list-jobs'            # args: <username>
    MCL_INFO = 'mcl-job-info'             # args: <job-id>
    MCL_DEL_JOB = 'mcl-delete-job'        # args: <job-id>
    NEED_SUDO = False

    BASE_DIR = os.getcwd() # see mtods

    def __init__(self):
        '''Check if we're working.'''
        self.__test_and_configure()

    def __test_and_configure(self):
        '''Execute a sample drush comandline and checks the output.
        
        First execution is silent. If we fail, either config is wrong or we need
        more priviledges, so we try again with sudo. If we still fail, construct a
        nice error message, and dump the output.
        '''
        status, out = self.execute('pml', '', nodump=True, quiet=True)
        if status == 0:
            return
        else:
            self.NEED_SUDO = True
            status, out = self.execute('pml', '')
            if status != 0:
                self.__dump_output('drush_fail.log', out)
                msg = '[-] drush exited with {0}'.format(status)
                raise RuntimeError(msg)

    def __dump_output(self, name, dump, q=False):
        '''Create a dumpfile named <name>, located at <self.BASE_DIR>. If the file
        exists, we prefix the current date and time.
        '''
        tmp = '{path}/{opt}{f}'
        fd = tmp.format(path=self.BASE_DIR, opt='', f=name)
        if os.path.exists(fd):
            now = time.strftime('%m_%d_%H-%M-%S_', time.localtime())
            fd = tmp.format(path=self.BASE_DIR, opt=now, f=name)
        with open(fd, 'w') as fd:
            fd.write(dump)
        if not q:
            print '[-] dump written "{}"'.format(fd)

    def execute(self, command, arguments, nodump=False, quiet=False):
        '''Creates a drush command line, executes it and returns a
        tuple(status, output).
        '''
        sudo = ''
        if self.NEED_SUDO:
            sudo = 'sudo'
        drush_cmd = self.TEMPLATE.format(sudo=sudo, binary=self.BINARY,
                                         root=self.DRUPAL_PATH, cmd=command,
                                         args=arguments)
        if self.NEED_SUDO:
            print '[!] ' + drush_cmd

        status, out = getstatusoutput(drush_cmd)

        if not out:
            return status, None
        if not nodump and '[error]' in out:
            self.__dump_output('drush_{}.log'.format(command), out, quiet)

        if status == 0:
            out = self.__known_output(out)

        return status, out

    def __known_output(self, o):
        '''Check if we understand the output and can format it nicer.'''
        ls = o.split('\n')
        if 'Job Info' in ls[0]:
            o = self.__mcl_parse_jobinfo(ls)
        return o

    def __mcl_parse_jobinfo(self, ls):
        '''Parse MCL jobinfo command output as dict().'''
        d = dict()
        for l in ls:
            #if re.match(':\s*$', l): # then subdict, but is not.. important
            spl = l.split(' : ')
            if '=' in spl[0]:
                spl = spl[0].split('=')
            if len(spl) != 2:
                join = ''.join(spl).lstrip().rstrip()
                if not join: continue
                d.update({join:''})
                continue
            d.update({spl[0].lstrip().rstrip():spl[1].lstrip().rstrip()})
        return d

