'''As keeping track of all the data we inter into Chado is tedious, we just
rollback the whole chado schema.'''
import os
import datetime, time
try:
    from commands import getstatusoutput
except ImportError:
    from subprocess import getstatusoutput

PJ_PATH = os.path.dirname(sys.argv[0])

# TODO make that sudo v configurable!
# TODO {db} needs to be configurable too!
class PostgreRestorer():
    '''Python Wrapper for the pg_dump and pg_restore cmd-line tools.'''
    c_dump = 'sudo -u postgres pg_dump -Fc {db}'
    c_drop = 'sudo -u postgres psql {db} < '+PJ_PATH+'/sql/delete.sql'
    c_crea = ':' # dummy, cause no need for this any longer
    c_res = 'sudo -u postgres pg_restore -j 16 --schema=chado --dbname={db} '

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

    def __known_special_case(self, s, o):
        '''Check status and output for known (ignorable) values.'''
        lines = o.split('\n')
        if 'WARNING: errors ignored' in lines[-1]:
            return True # no problem
        return False

    def __exe_c(self, cmd):
        '''Print execute, and throw on non-0 return value.'''
        cmd = cmd.format(db=self.db)
        s, o = getstatusoutput(cmd)
        if s != 0:
            msg = '[!] cmd $({cmd}) returned "{val}":\n{out}'
            msg = msg.format(cmd=cmd, out=str(o)[:1000], val=s)
            if self.__known_special_case(s, o):
                print '[warning:{0}] {1}'.format(s, msg)
            else:
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
        '''Restore current chado data of our DB, returns True on success.'''
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
                    self.__exe_c(self.c_res + self.dumpfile) # does not work!
                    # but it should work.. 
                    self.__exe_c(self.c_res + self.dumpfile) # does work sometimes!
                    # but no still not reliable..
                    self.__exe_c(self.c_res + self.dumpfile) # fixed!
                    # Speculations what happened here:
                    #  - pg_restore tool can't handle chado's complexity
                    #  - ?
                    res_success = True
                break
            except Exception as e:
                print '[.restore] failed cuz {}'.format(e)
                try:
                    raw_input('[.restore] Try fixit and press <Enter>')
                except KeyboardInterrupt:
                    return False
                except EOFError:
                    continue
        else:
            return False
        return True

