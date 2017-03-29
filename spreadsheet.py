'''Excel Spreadsheet Utility, mostly Mainlab Chado Loader (MCL) compatible
speadsheet creation.'''

# Spreadsheet writer (xls + xlsx).
import openpyxl

class MCLSpreadsheet():
    '''Writes MCL compatible spreadsheets for chado import.'''

    OUTFILE = 'test.xlsx'

    DB_HEADERS = [
        '*db_name', 'url_prefix', 'url', 'description'
    ]
    CV_HEADERS = [
        '*cv_name', 'definition'
    ]
    CVTERM_HEADERS = [
        '*db_name', '*cv_name', '*cvterm_name', '*accession', 'definition'
    ]
    STOCK_HEADERS = [
        '*stock_name', '*germplasm_type', '*genus', '*species', 'secondary_ID',
        'description', 'subspecies', 'GRIN_ID', 'paternal_parent',
        'maternal_parent', 'mutation_parent', 'selfing_parent', 'alias',
        'cultivar', 'pedigree', 'origin', 'population_size',
        'germplasm_center', 'comments', 'image', 'reference'
    ]
    DATASET_HEADERS = [
        '*dataset_name', '*type', 'sub_type', 'super_dataset',
        'trait_descriptor_set', 'PI', 'crop', 'comments', 'reference',
        'permission', 'description'
    ]
    PHENO_HEADERS = [
        '*dataset_name', '*stock_name', '*genus', '*species', '*sample_ID',
        'clone_ID', 'evaluator', 'site_name', 'rep', 'rootstock', 'plot',
        'row', 'position', 'plant_date', 'data_year', 'evaluation_date',
        'pick_date', 'fiber_pkg', 'storage_time', 'storage_regime', 'comments'
    ]

    def __init__(self):
        self.created_xlsxs = []

    def __create_xlsx(self, headers, content, title=''):
        '''Create a spreadsheet like this:
        
        N, M = len(headers), len(content)

             headers[0]     | headers[..]     | headers[N]
            ----------------+-----------------+---------------
             content[0][0]  | content[0][..]  | content[0][N]
             content[..][0] | content[..][..] | content[..][N]
             content[M][0]  | content[M][..]  | content[M][N]

        Note: The type(content) may be a list() or a dict(), in later case
              values are assigned to the columns indicated by the keys
              (numbers/letters).
        '''
        wb = openpyxl.Workbook()
        ws1 = wb.get_active_sheet()
        if title: ws1.title = title

        for n, header in enumerate(headers):
            c = ws1.cell(1, n)
            c.value = header

        for row in content:
            ws1.append(row)

        self.created_xlsxs += wb
        return wb

    def create_TYPE(self, filename, content, TYPE):
        '''Create, save, and return a <TYPE> spreadsheet with content.

        The TYPE is an array used as header. We supply some MCL defaults:
            self.DB_HEADERS
            self.CV_HEADERS
            self. ..

        Note: The type(content) may be a list() or a dict(), in later case
              values are assigned to the columns indicated by the keys
              (numbers/letters).
        '''
        wb = self.__create_xlsx(TYPE, content)
        wb.save(filename)
        return wb

    def create_db(self, filename, name, description=''):
        '''Convenience wrapper around create_TYPE()'''
        content = [name, '', '', description]
        return self.create_TYPE(filename, content, self.DB_HEADERS)

    def create_cv(self, filename, name, description=''):
        '''Convenience wrapper around create_TYPE()'''
        content = [name, description]
        return self.create_TYPE(filename, content, self.CV_HEADERS)

    def create_cvterm(self, filename, dbname, cvname, cvterm, accession=[],
        definition=[]):
        '''Convenience wrapper around create_TYPE()
        
        By default accessesion will be set equal to cvterm and definition will
        be empty.

        Arguments:                  Types:
            dbname, cvname              str
            cvterm,accession,defi..     [cvterm_name1, cvterm_name2, ..]
        '''
        if not accession:
            accession = cvterm

        if len(cvterm) != len(accession):
            raise RuntimeError('[.create_cvterm] argument length inequal!')
        if definition:
            if len(cvterm) != len(accession) or len(cvterm) != len(definition):
                raise RuntimeError('[.create_cvterm] argument length inequal!')

        if not definition:
            it = zip(cvterm, accession)
            content = [[dbname, cvname, c, a, ''] for c,a in it]
        else:
            it = zip(cvterm, accession, definition)
            content = [[dbname, cvname, c, a, d] for c,a,d in it]

        return self.create_TYPE(filename, content, self.CVTERM_HEADERS)

    #def create_*ORGANISM*():
        # TODO: We need this function, but MCL has no template for it.

    def create_stock(self, filename, name, germplasm_type, genus, species):
        '''Convenience wrapper around create_TYPE()
        
        The 'secondary_ID' will be set equal to name (germplasm name).
        '''
        content = [[g, germplasm_type, genus, species, g] for g in name]
        return self.create_TYPE(filename, content, self.STOCK_HEADERS)

    def create_dataset(self, filename, dataset_name, type, sub='', super=''):
        '''Convenience wrapper around create_TYPE()
        
        A Project/Dataset/Experiment, or just a set of data.
        '''
        content = [dataset_name, type, sub, super]
        return self.create_TYPE(filename, content, self.STOCK_HEADERS)

    def upload_phenotype(self, dataset_name, stock, sample_id, clone_id,
        descriptors, genus='', species='', contact=''):
        '''Upload Phenotype Data.
        
        Arguments:
            - stock, genus, species: If stock is specified, genus and species
              MIGHT be unambiguous, otherwise they MUST be specified, too.
            - sample_id: MUST be unique, CAN be constructed from other attrib's
            - clone_id and contact: CAN be omitted
            - descriptors
        '''
        # TODO Writeon..
        #if not genus: genus = ??
        #if not species: species = ??
        content = []#dataset_name, stock, genus, species, ..
        return self.create_TYPE(filename, content, self.STOCK_HEADERS)

