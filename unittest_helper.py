'''As we don't know excactly what Mainlab Chado Loader does with our database,
we just save and rollback the whole test database for our unittests. Sounds
reasonable.'''
import os
import datetime, time
try: # Calling drush, we want status and ouput
    from commands import getstatusoutput
except ImportError:
    from subprocess import getstatusoutput

class PostgreRestorer():
    '''Python Wrapper for the pg_dump and pg_restore cmd-line tools.'''
    c_dump = 'sudo -u postgres pg_dump -Fc {db}'
    c_drop = 'sudo -u postgres dropdb {db}'
    c_crea = 'sudo -u postgres createdb {db}'
    c_res = 'sudo -u postgres pg_restore --dbname={db} '

    # Outside of project folder cuz of paranoia.
    MASTERDUMP = os.path.join(os.path.expanduser('~'), 'ciat', 'ALLDB.dump')

    def __init__(self, db='drupal7', basedir='', fname='chado.dump'):
        self.basedir = basedir
        self.dumpfile = os.path.join(self.basedir, fname)
        if os.path.exists(self.dumpfile):
            now = time.strftime('%m_%d_%H-%M-%S_', time.localtime())
            self.dumpfile = now + self.dumpfile
        self.db = db

        self.__check_masterdump()

    def __check_masterdump(self, days=2):
        '''Check for existence of a complete dump in the last <days>.'''
        if not os.path.exists(self.MASTERDUMP):
            msg = 'FAIL, no masterdump @{}'.format(self.MASTERDUMP)
            raise RuntimeError(msg)
        stat = os.stat(self.MASTERDUMP)
        m_time = datetime.datetime.fromtimestamp(stat.st_mtime)
        now = datetime.datetime.now()
        if (now - m_time).days > days:
            msg = 'Should renew your {}'.format(self.MASTERDUMP)
            raise Warning(msg)

    def __exe_c(self, cmd):
        '''Print execute, and throw on non-0 return value.'''
        cmd = cmd.format(db=self.db)
        s, o = getstatusoutput(cmd)
        if s != 0:
            msg = 'Could not execute cmd $({cmd}) returned "{val}":\n{out}'
            msg = msg.format(cmd=cmd, out=str(o)[:1000], val=s)
            raise RuntimeError(msg)
        else:
            print '[+] {}'.format(cmd)
        return o

    def dump(self):
        '''Dump current chado data of our DB.'''
        o = self.__exe_c(self.c_dump)
        fd = open(self.dumpfile, 'w')
        fd.write(o)
        fd.close()

    def restore(self):
        '''Restore current chado data of our DB.'''
        drop_success, crea_success, res_success = False, False, False
        for tries in range(3):
            try:
                if not drop_success:
                    self.__exe_c(self.c_drop)
                    drop_success = True
                if not crea_success:
                    self.__exe_c(self.c_crea)
                    crea_success = True
                if not res_success:
                    self.__exe_c(self.c_res + self.dumpfile)
                    res_success = True
                break
            except Exception as e:
                print '[.restore] failed cuz {}'.format(e)
                print '[.restore] Try fixit and press <Enter>'
                input()

