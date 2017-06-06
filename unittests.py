#!/usr/bin/python
import unittest
import traceback
from unittest_helper import PostgreRestorer as PGR
import chado
import os
import cx_oracle
import cassava_ontology
import getpass
import migration
import table_guru
import utility

PATH='testpath'

# Sanity checks.
if not os.path.exists(PATH):
    os.mkdir(PATH)

# Half-Global connections to speed things up a lot.
# Note that the other test will use these connections.
class ConTest(unittest.TestCase):
    # Get Chado-,Oracle -DB connections, and the Ontology
    chadodb = chado.ChadoPostgres(host='127.0.0.1', usr='drupal7')
    linker = chado.ChadoDataLinker(chadodb, 'mcl_pheno', 'mcl_pheno')
    oracledb = cx_oracle.Oracledb()
    oracledb.connect()
    cass_onto = cassava_ontology.CassavaOntology(oracledb.cur)
    # Asure we got it.
    def test_connections_not_none(self):
        self.assertIsNotNone(self.chadodb)
        self.assertIsNotNone(self.oracledb)
        self.assertIsNotNone(self.oracledb.con)
        self.assertIsNotNone(self.oracledb.cur)
        self.assertIsNotNone(self.linker)

class PostgreTests(unittest.TestCase):
    longMessage = True

    # set variables used in tearDown
    @classmethod
    def setUpClass(cls):
        cls.cvts = ['abc', 'def']
        cls.stocks = ['12 GM 2319023 z', '12 GM 2319023 lsd']
        cls.sites = [
                {'nd_geolocation.description' : 'asdfAA',
                 'nd_geolocation.altitude'    : '1',
                 'nd_geolocation.latitude'    : '666N',
                 'nd_geolocation.longitude'   : ' 73W'},
                {'nd_geolocation.description' : 'asdfAAr2',
                 'nd_geolocation.altitude'    : '2',
                 'nd_geolocation.latitude'    : ' 038',
                 'nd_geolocation.longitude'   : '87'},
            ]
        cls.sites_clean = [ # used for deletion, and tests
                {'nd_geolocation.description' : 'asdfAA',
                 'nd_geolocation.altitude'    : '1',
                 'nd_geolocation.latitude'    : '666',
                 'nd_geolocation.longitude'   : '-73'},
                {'nd_geolocation.description' : 'asdfAAr2',
                 'nd_geolocation.altitude'    : '2',
                 'nd_geolocation.latitude'    : '38',
                 'nd_geolocation.longitude'   : '87'},
            ]
        cls.pheno_args = [
                cls.stocks,
                [{'some_property' : 777},
                 {'some_property' : 777}]
            ]
        cls.pheno_kwargs = {
                'others' : [{'pick_date': 12, 'plant_date' : 13},
                            {'pick_date': 21, 'plant_date' : 31}]
            }
        cls.props = [
                [cls.stocks[0], 'Avaluuea',],
                [cls.stocks[1], 'Bvaluauaeas',],
            ]
        cls.props_type = 'icontain'
 
    # remove all the things
    # sadly we cannot: ConTest.chadodb.con.rollback()
    @classmethod
    def tearDownClass(cls):
        # get a new cursor in case something went wrong
        ConTest.chadodb.con.commit()
        ConTest.chadodb.c = ConTest.chadodb.con.cursor()
        for c in cls.cvts:
            ConTest.chadodb.delete_cvterm(c, cv=ConTest.linker.cv,
                                          and_dbxref=True)
        for s in cls.stocks:
            ConTest.chadodb.delete_stock(s)
        for g in cls.sites_clean:
            ConTest.chadodb.delete_geolocation(keys=g)
        for sps in cls.props:
            ConTest.chadodb.delete_stockprop(val=sps[1], type=cls.props_type)
        for p in cls.pheno_args[1]:
            ConTest.chadodb.delete_phenotype(keyval=p, del_attr=True)
        for spp in cls.pheno_kwargs['others']:
            ConTest.chadodb.delete_stockprop(keyval=spp)

    def test_organism_funcs(self):
        genus = 'test_genus'
        species = 'test_species'
        ConTest.chadodb.create_organism(genus, species)
        self.assertTrue(ConTest.chadodb.has_species(species))
        self.assertTrue(ConTest.chadodb.has_genus(genus))
        ConTest.chadodb.delete_organism(genus, species)
        self.assertFalse(ConTest.chadodb.has_species(species))
        self.assertFalse(ConTest.chadodb.has_genus(genus))

    def test_cvterm_tasks(self):
        ts = ConTest.linker.create_cvterm(self.cvts)
        pre_len = len(ConTest.chadodb.get_cvterm())
        for t in ts:
            t.execute()
        post_len = len(ConTest.chadodb.get_cvterm())
        msg = 'creation of cvterms failed'
        self.assertEqual(pre_len + 2, post_len, msg)
        cvterms = [i.name for i in ConTest.chadodb.get_cvterm()]
        self.assertIn(self.cvts[0], cvterms, msg)
        self.assertIn(self.cvts[1], cvterms, msg)
        msg = 'creation of dbxref accession failed'
        dbxrefs = [i.accession for i in ConTest.chadodb.get_dbxref()]
        self.assertIn(self.cvts[0], dbxrefs, msg)
        self.assertIn(self.cvts[1], dbxrefs, msg)

    def test_stock_tasks(self):
        organism = ConTest.chadodb.get_organism()[0]
        ts = ConTest.linker.create_stock(self.stocks, organism)

        pre_len = len(ConTest.chadodb.get_stock())
        for t in ts:
            t.execute()
        post_len = len(ConTest.chadodb.get_stock())
        msg = 'creation of stocks failed'
        self.assertEqual(pre_len + 2, post_len, msg)
        stocks = [i.uniquename for i in ConTest.chadodb.get_stock()]
        self.assertIn(self.stocks[0], stocks, msg)
        self.assertIn(self.stocks[1], stocks, msg)

    def test_geolocation_tasks(self):
        ts = ConTest.linker.create_geolocation(self.sites)
        pre_len = len(ConTest.chadodb.get_nd_geolocation())
        for t in ts:
            t.execute()
        post_len = len(ConTest.chadodb.get_nd_geolocation())
        msg = 'creation of geolocations failed'
        self.assertEqual(pre_len + 2, post_len, msg)
        sts = [i.description for i in ConTest.chadodb.get_nd_geolocation()]
        self.assertIn(self.sites[0]['nd_geolocation.description'], sts, msg)
        self.assertIn(self.sites[1]['nd_geolocation.description'], sts, msg)
        lats = [i.latitude for i in ConTest.chadodb.get_nd_geolocation()]
        msg = 'translation of coordinates failed'
        self.assertIn(float(self.sites_clean[1]['nd_geolocation.latitude']),
                      lats, msg)

    def test_stockprop_tasks(self):
        vals = ','.join("'"+s+"'" for s in self.stocks)
        where = "uniquename = ANY(ARRAY[{}])".format(vals)
        stocks = ConTest.chadodb.get_stock(where=where)
        if not len(stocks) == 2:
            msg = 'Cannot execute stockprop test, as stock test failed'
            raise RuntimeError(msg)
        ts = ConTest.linker.create_stockprop(self.props, self.props_type)
        pre_len = len(ConTest.chadodb.get_stockprop())
        for t in ts:
            t.execute()
        post_len = len(ConTest.chadodb.get_stockprop())
        msg = 'stockprop creation failed'
        self.assertEqual(pre_len + 2, post_len, msg)
        stockprops = ConTest.chadodb.get_stockprop()
        a = [i for i in stockprops if i.value == self.props[0][1]]
        b = [i for i in stockprops if i.value == self.props[1][1]]
        msg = 'stockprop value not found'
        self.assertTrue(a != [], msg)
        self.assertTrue(b != [], msg)
        a = a[0]
        b = b[0]
        stocks = ConTest.chadodb.get_stock()
        a_stock = [i for i in stocks if i.uniquename == self.stocks[0]]
        b_stock = [i for i in stocks if i.uniquename == self.stocks[1]]
        a_id = a_stock[0].stock_id
        b_id = b_stock[0].stock_id
        msg = 'stockprop ordering failed'
        self.assertEqual(a.stock_id, a_id, msg)
        self.assertEqual(b.stock_id, b_id, msg)

    @staticmethod
    def print_tasks(ts, pre=''):
        if type(ts) == list:
            for t in ts:
                print PostgreTests.print_tasks(t, pre=pre)
        elif type(ts) == tuple:
            for t in ts:
                print PostgreTests.print_tasks(t, pre=pre+'\t')
        else:
            if pre != '': pre = pre + '>'
            print pre, str(ts)[:50]+'...'

    def test_phenotype_tasks(self):
        ts = ConTest.linker.create_phenotype(*self.pheno_args,
                                             **self.pheno_kwargs)
        print '\n=== Tasks Start ==='
        self.print_tasks(ts)
        print '\n=== Tasks End ===\n'
        pre_len = len(ConTest.chadodb.get_phenotype())
        utility.Task.non_parallel_upload(ts)
        post_len = len(ConTest.chadodb.get_phenotype())
        msg = 'creation of phenotypes failed'
        # expecting at least 3 valid traits for the 2 v testuploads
        self.assertGreaterEqual(post_len, pre_len + (2*3), msg)


class OracleTests(unittest.TestCase):
    longMessage = True # Append my msg to default msg.
    cass_onto = ConTest.cass_onto
    def test_tablegurus_ontology_creation(self):
        onto, onto_sp = self.cass_onto.onto, self.cass_onto.onto_sp
        cvt0 = 'Numero de plantas cosechadas'
        self.assertEqual(onto_sp[0].SPANISH, cvt0, 'First cvt changed, bad!')
        self.assertEqual(onto_sp[0].COLUMN_EN, 'NOHAV')
        self.assertEqual(len(onto_sp), 24, 'Size changed, interesting.')
        self.assertEqual(len(onto), len(set(onto)), 'Double entries, bad!')

class BigTest(unittest.TestCase):
    '''Monolitic tests, building up some state.'''
    enableThisMonoliticTestWithLongDuration = False

    # Append my msg to default msg.
    longMessage = True 

    # Number of lines imported, this should directly correlate to the added
    # rows of phenotyping data in chado. If 'None' all data will be used.
    NTEST = 100

    def step10_stateful_setup(self):
        self.done_pg_backup = False
        self.need_rollback = False
        self.done_restore = False
        self.oracle = ConTest.oracledb
        self.t1 = migration.Migration.TABLES_MIGRATION_IMPLEMENTED[0]
        self.tg = table_guru.TableGuru(self.t1, self.oracle, basedir=PATH)
        self.pgr = PGR()

    def step20_inside_tests(self):
        if not self.need_rollback:
            print '[-] cannot do inside tests, as no data was uploaded'
            return
        msg = 'should have at least the amount of phenotypes, that we uploaded'\
            + 'as rows, realistically it should be a multiple of that number'
        minimum = self.n_phenos0 + self.NTEST * 2
        curr_phenos = ConTest.chadodb.count_from('phenotype')
        self.assertGreaterEqual(curr_phenos, minimum, msg)

    def step90_stateful_teardown(self):
        self.cleanup()

    def step91_after_tests(self):
        # Open connection again to test, if the state reverted properly.
        ConTest.chadodb._ChadoPostgres__connect(chado.DB, chado.USER,
                                                chado.HOST, chado.PORT)
        stocks = ConTest.chadodb.get_stock()
        msg = 'PG Restore failed! You should restore the DB manually.'
        self.assertEqual(self.n_stocks, len(stocks), msg)
        self.assertEqual(self.n_phenos0, ConTest.chadodb.count_from('phenotype'), msg)

    def cleanup(self, wait_for_inp=True):
        '''Wait for <Return>, then check if we need and can do a db rollback, if
        so: do it.'''
        if wait_for_inp:
            print 'Press <Return> to restore the database.'
            try:
                input()
            except Exception:
                pass

        for s in self.sprds:
            SpreadsheetTests.rm(s)

        if not self.done_pg_backup and self.need_rollback:
            print '[-] Need manual db rollback.. '
        if self.done_pg_backup and self.need_rollback and not\
                self.done_restore:
            # Close all connections before we can restore..
            ConTest.chadodb.c.close()
            ConTest.chadodb.con.close()
            self.tg.chado.c.close()
            self.tg.chado.con.close()
            # now restore..
            self.pgr.restore()
            self.done_restore = True
        if self.done_restore:
            os.remove(self.pgr.dumpfile)

    def _steps(self):
        for name in sorted(dir(self)):
            if name.startswith("step"):
                yield name, getattr(self, name)

    def test_steps(self):
        if BigTest.enableThisMonoliticTestWithLongDuration:
            for name, step in self._steps():
                try:
                    step()
                except Exception as e:
                    traceback.print_exc(e)
                    self.cleanup()
                    break

    def __get_pheno_count(self):
        return ConTest.chadodb.count_from('phenotype')


def run():
    ts = unittest.TestSuite()
    tl = unittest.TestLoader()
    ts.addTest(tl.loadTestsFromTestCase(ConTest))
    ts.addTest(tl.loadTestsFromTestCase(PostgreTests))
    ts.addTest(tl.loadTestsFromTestCase(OracleTests))
    ts.addTest(tl.loadTestsFromTestCase(BigTest))

    runner = unittest.TextTestRunner()
    runner.run(ts)

if __name__ == '__main__':
    run()
