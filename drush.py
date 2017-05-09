''' '''
import os
import time
#import re
try: # Calling drush, we want status and ouput
    from commands import getstatusoutput
except ImportError:
    from subprocess import getstatusoutput

DEFAULT_USR = 'admin'

class Drush():
    '''Wrapper around the shell programm 'drush'.

    Contains default config, MainlabChadoLoader commands (mcl).

    Note: Needs to be configured manually for the current drupal AND drush
          installation.
    '''
    BINARY = '/usr/bin/drush'
    TEMPLATE = '{sudo} {binary} --root={root}'
    DRUPAL_PATH = '/usr/share/drupal7'
    MCL_UPLOAD = 'mcl-upload-data {usr} {file}'
    MCL_LIST = 'mcl-list-jobs {usr}'
    MCL_INFO = 'mcl-job-info {job_id}'
    MCL_DEL_JOB = 'mcl-delete-job {job_id}'
    NEED_SUDO = False

    BASE_DIR = os.getcwd() # see mtods

    def __init__(self, usr=DEFAULT_USR, nodump=True):
        '''Also checks if we're good to go.'''
        self.nodump = nodump
        self.usr = usr
        self.MCL_UPLOAD = self.MCL_UPLOAD.format(usr=self.usr, file='{file}')
        self.MCL_LIST = self.MCL_LIST.format(usr=self.usr)

        # temporary set drush_cmd to execute test_and_configure()
        self.__set_drush_cmd()
        self.__test_and_configure()

    def __set_drush_cmd(self):
        '''This'''
        if self.NEED_SUDO:
            sudo = 'sudo'
        else:
            sudo = ''
        self.drush_cmd = self.TEMPLATE.format(sudo=sudo, binary=self.BINARY,
                                              root=self.DRUPAL_PATH)
        self.drush_cmd = self.drush_cmd + ' {cmd}'

    def __test_and_configure(self):
        '''Execute a sample drush comandline and checks the output.
        
        First execution is silent. If we fail, either config is wrong or we need
        more priviledges, so we try again with sudo. If we still fail, construct a
        nice error message, and dump the output.
        '''
        status, out = self.execute('pml', quiet=True)
        if status == 0:
            return
        else:
            self.NEED_SUDO = True
            self.__set_drush_cmd()

            tmp = self.nodump
            self.nodump = True
            status, out = self.execute('pml')
            self.nodump = tmp

            if status != 0:
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
            print "[-] dump written to '{}'".format(fd.name)

    def execute(self, cmd, quiet=False):
        '''Creates a drush cmd line, executes it and returns a
        tuple(status, output).
        '''
        exe_cmd = self.drush_cmd.format(cmd=cmd)
        if self.NEED_SUDO:
            print '[!] {}'.format(exe_cmd)
        else:
            print '[$] {}'.format(exe_cmd)

        status, out = getstatusoutput(exe_cmd)

        if not out:
            return status, None
        if not self.nodump and '[error]' in out:
            dfname = 'drush_{}.log'.format(cmd.split(os.path.sep)[0])
            self.__dump_output(dfname, out, quiet)

        if status == 0:
            out = self.__known_output(out)

        return status, out

    def mcl_upload(self, fd):
        '''Convenient wrapper around execute.'''
        cmd = self.MCL_UPLOAD.format(file=fd)
        return self.execute(cmd)

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

