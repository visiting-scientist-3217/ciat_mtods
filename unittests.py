#!/usr/bin/python
import unittest
from unittest_helper import PostgreRestorer as PGR
import spreadsheet
import chado
import os
import cx_oracle
import cassava_ontology
import getpass
import migration
import table_guru

PATH='testpath'

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

# Sanity checks.
if not os.path.exists(PATH):
    os.mkdir(PATH)

class SpreadsheetTests(unittest.TestCase):

    KEEP_FILES = True
    chado_cursor = chado.ChadoPostgres()
    s = spreadsheet.MCLSpreadsheet(chado_cursor)

    @staticmethod
    def rm(file):
        if not SpreadsheetTests.KEEP_FILES:
            os.remove(file)

    def test_cv(self):
        fd = os.path.join(PATH, 'cv.xlsx')
        w = self.s.create_cv(fd, 'im a cv name', 'im the description')
        s = w.get_active_sheet()
        self.assertTrue(os.path.exists(fd))
        self.assertEqual(s.cell(coordinate='A1').value, '*cv_name')
        self.assertEqual(s.cell(coordinate='B1').value, 'definition')
        self.rm(fd)
    def test_db(self):
        fd = os.path.join(PATH, 'db.xlsx')
        w = self.s.create_db(fd, 'im a db', 'dscsalkjdals')
        s = w.get_active_sheet()
        self.assertTrue(os.path.exists(fd))
        self.assertEqual(s.cell(coordinate='A1').value, '*db_name')
        self.assertEqual(s.cell(coordinate='D1').value, 'description')
        self.rm(fd)
    def test_cvterm(self):
        fd = os.path.join(PATH, 'cvterm.xlsx')
        db, cv = 'db', 'cv'
        cvt = ['somecvterm', 'anotherone', 'andathirdcvterm']
        w = self.s.create_cvterm(fd, db, cv, cvt)
        s = w.get_active_sheet()
        self.assertEqual(s.cell(coordinate='C1').value,
            spreadsheet.MCLSpreadsheet.CVTERM_HEADERS[2])
        self.assertEqual(s.cell(coordinate='C2').value, cvt[0])
        self.rm(fd)
    def test_cvterm2(self):
        fd = os.path.join(PATH, 'cvterm2.xlsx')
        db, cv = 'db', 'cv'
        cvt = ['somecvterm', 'anotherone', 'andathirdcvterm']
        acs = [i+' acs' for i in cvt]
        dsc = [i+' dsc' for i in cvt]
        s = self.s.create_cvterm(fd, db, cv, cvt, acs, dsc).get_active_sheet()
        self.assertEqual(s.cell(coordinate='C2').value, cvt[0])
        self.assertEqual(s.cell(coordinate='D2').value, acs[0])
        self.assertEqual(s.cell(coordinate='E2').value, dsc[0])
        self.assertEqual(s.cell(coordinate='C3').value, cvt[1])
        self.assertEqual(s.cell(coordinate='D3').value, acs[1])
        self.assertEqual(s.cell(coordinate='E3').value, dsc[1])
        self.rm(fd)
    def test_phenotype(self):
        fd = os.path.join(PATH, 'phenotype.xlsx')
        dn, sk, ge, sp = 'some dataset', 'CK 13 33 K', 'genus', 'species'
        dcs = [
            {'#st':'stv1', '#ano1':'ano1v'},
            {'#ano2':'ano2v', '#st':'stv2'}
        ]
        s = self.s.create_phenotype(fd, dn, sk, dcs, genus=ge,
                                    species=sp).get_active_sheet()
        self.assertEqual(s.cell(coordinate='E2').value, 'st_stv1')
        headers = [h.value for h in s.rows[0]]
        for d in dcs:
            for key in d:
                self.assertIn(key, headers)
        self.rm(fd)

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

class GuruTest(unittest.TestCase):
    longMessage = True # Append my msg to default msg.
    oracle = ConTest.oracledb
    t1 = migration.Migration.TABLES_MIGRATION_IMPLEMENTED[0]
    tg = table_guru.TableGuru(t1, oracle, basedir=PATH)
    pgr = PGR()
    def test_translation_of_stock(self):
        self.tg.do_upload = False
        #self.tg.VERBOSE = True
        self.sprds += self.tg.create_workbooks(test=10)
        self.assertEqual(self.sprds[0][-11:], 'stocks.xlsx', 'Cannot happen.')
    def test_stock_upload(self):
        # TODO implement the upload test, remember to rollback the database
        # with self.pgr.dump() .restore()
        pass
    def test_cleanup(self):
        '''Not a test, just cleanup.'''
        for s in self.sprds:
            SpreadsheetTests.rm(s)


def run():
    unittest.main()

if __name__ == '__main__':
    run()
