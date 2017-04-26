'''As we don't know excactly what Mainlab Chado Loader does with our database,
we just save and rollback the whole test database for our unittests. Sounds
reasonable.'''
import os
import datetime
try: # Calling drush, we want status and ouput
    from commands import getstatusoutput
except ImportError:
    from subprocess import getstatusoutput

class PostgreRestorer():
    '''Python Wrapper for the pg_dump and pg_restore cmd-line tools.'''
    c_dump = 'sudo -u postgres pg_dump --data-only -Fc {db} -t "chado.*"'
    c_res = 'sudo -u postgres pg_restore --data-only --dbname={db} {dumpfile}'

    MASTERDUMP = os.path.join(os.path.expanduser('~'), 'ciat', 'ALLDB.dump')

    def __init__(self, db='drupal7', basedir='', fname='chado.dump'):
        self.basedir = basedir
        self.dumpfile = os.path.join(self.basedir, fname)
        self.c_dump = self.c_dump.format(db=db)
        self.c_res = self.c_res.format(db=db, dumpfile=self.dumpfile)
        self.__check_masterdump()

    def __check_masterdump(self, days=3):
        '''Check for existence of a complete dump in the last <days>.'''
        if not os.path.exist(self.MASTERDUMP):
            msg = 'FAIL, no masterdump @{}'.format(self.MASTERDUMP)
            raise RuntimeError(msg)
        stat = os.stat(self.MASTERDUMP)
        m_time = datetime.datetime.fromtimestamp(stat.st_mtime)
        now = datetime.datetime.now()
        if (now - stamp).days > days:
            msg = 'Should renew your {}'.format(self.MASTERDUMP)
            raise Warning(msg)

    def dump(self):
        s, o = getstatusoutput(self.c_dump)
        if s != 0:
            raise RuntimeError('Could not dump postgres')
        else:
            if os.path.exists(self.dumpfile):
                now = time.strftime('%m_%d_%H-%M-%S_', time.localtime())
                self.dumpfile = now + self.dumpfile
            fd = open(self.dumpfile, 'w')
            fd.write(o)
            fd.close()
            print '[+] dump\'ed postgres'

    def restore(self):
        s, o = getstatusoutput(self.c_res)
        if s != 0:
            raise RuntimeError('Could not restores postgres')
        else:
            print '[+] restores postgres'

