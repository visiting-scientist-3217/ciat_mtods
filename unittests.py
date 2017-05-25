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

PATH='testpath'

# Sanity checks.
if not os.path.exists(PATH):
    os.mkdir(PATH)

# Half-Global connections to speed things up a lot.
# Note that the other test will use these connections.
class ConTest(unittest.TestCase):
    chadodb = chado.ChadoPostgres(host='127.0.0.1', usr='drupal7')
    oracledb = cx_oracle.Oracledb()
    oracledb.connect()
    cass_onto = cassava_ontology.CassavaOntology(oracledb.cur)
    def test_connections_not_none(self):
        self.assertIsNotNone(self.chadodb)
        self.assertIsNotNone(self.oracledb)
        self.assertIsNotNone(self.oracledb.con)
        self.assertIsNotNone(self.oracledb.cur)

class PostgreTests(unittest.TestCase):
    def test_organism_funcs(self):
        genus = 'test_genus'
        species = 'test_species'
        ConTest.chadodb.create_organism(genus, species)
        self.assertTrue(ConTest.chadodb.has_species(species))
        self.assertTrue(ConTest.chadodb.has_genus(genus))
        ConTest.chadodb.delete_organism(genus, species)
        self.assertFalse(ConTest.chadodb.has_species(species))
        self.assertFalse(ConTest.chadodb.has_genus(genus))

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

# To enable this, set enableThisMonoliticTestWithLongDuration to True.
class BigTest(unittest.TestCase):
    '''Monolitic tests, building up some state.'''
    # Append my msg to default msg.
    longMessage = True 

    # Create spreadsheets for a real migration.
    enableThisMonoliticTestWithLongDuration = True

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
