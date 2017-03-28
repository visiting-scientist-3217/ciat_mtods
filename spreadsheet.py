'''Excel Spreadsheet Utility, mostly Mainlab Chado Loader (MCL) compatible
speadsheet creation.'''

# Spreadsheet writer (xls + xlsx).
import openpyxl

class MCLSpreadsheet():
    '''Writes MCL compatible spreadsheets for chado import.'''

    def create_db(filename, name):
        pass

    def create_cv(filename, name):
        pass

    def create_cvterm(filename, dbname, cvname, cvterm, accession=[], definition=[]):
        '''By default accessesion will be set equal to cvterm and definition will
        be empty.

        Arguments:                  Types:
            dbname, cvname              str
            cvterm,accession,defi..     [cvterm_name1, cvterm_name2, ..]
        '''
        pass

    def create_stock(filename, name, germplasm_type, genus, species):
        '''The 'secondary_ID' will be set equal to name (germplasm name).'''
        pass

    def create_dataset(dataset_name, type, sub_type='', super_dataset=''):
        '''A Project/Dataset/Experiment, or just a set of data.'''
        pass

    def upload_phenotype(dataset_name, stock, sample_id, clone_id, descriptors,
        genus='', species='', contact=''):
        '''Upload Phenotype Data.
        
        Arguments:
            - stock, genus, species: If stock is specified, genus and species
              might be unambiguous, otherwise they MUST be specified, too.
            - sample_id: MUST be unique, CAN be constructed from other attrib's
            - clone_id and contact: CAN be omitted
            - descriptors
        '''
        pass

