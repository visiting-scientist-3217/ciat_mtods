'''
Cassava Phenotyping Ontology

We need to know this for translating the columns to their chado equivalent.
'''

from collections import namedtuple
from utility import OracleSQLQueries as OSQLQ

def get_tabledata_as_tuple(cursor, table):
    '''Create a list of namedtuple's for <table> with <column_names> as
    members.
    
    Arguments:
        cursor      a PEP 249 compliant cursor pointing to the oracledb
        table       name of the table
    '''
    sql = OSQLQ.get_column_metadata_from.format(table=table)
    cursor.execute(sql)
    column_names = [line[1] for line in cursor.fetchall()]

    VOntology = namedtuple('VOntology', column_names)
    sql = OSQLQ.get_all_from.format(table=table)
    cursor.execute(sql)
    vonto = [VOntology(*line) for line in cursor.fetchall()]

    return vonto

class CassavaOntology():
    '''Ontology for the Cassava plant, in spanish and english.

    We also provide utilities for translation.

    Methods:
        get_ontologies()    Returns (onto, onto_sp)

    Members:
        onto_sp     Spanish raw ontology
        onto        English ontology (+cvterm meta info)
        mapping     Mapping of the spanish ontology names to a list of all
                    corresponding cvterms as VOntology() objects.
    '''

    SPANISH_ONTOLOGY_TABLE = 'V_ONTOLOGY_SPANISH'
    ONTOLOGY_TABLE = 'V_ONTOLOGY'

    def __init__(self, cursor):
        '''We need that cursor to the db holding chado.'''
        self.c = cursor
        self.onto_sp = get_tabledata_as_tuple(self.c,
            self.SPANISH_ONTOLOGY_TABLE)
        self.onto = get_tabledata_as_tuple(self.c, self.ONTOLOGY_TABLE)

        # Get all corresponding cvterm info for the spanish names
        tmp_map = [[i for i in self.onto \
            if i.VARIABLE_ID == j.VARIABLE_ID_BMS] for j in self.onto_sp]

        self.mapping = {}
        to_remove = []

        for key,value in zip(self.onto_sp, tmp_map):
            if len(value) == 0:
                # we did not find a mapping, so this cvterm is unusable
                to_remove.append(key)
            else:
                self.mapping[key.SPANISH] = value
        for term in to_remove:
            self.onto_sp.remove(term)

    def get_ontologies(self):
        '''Returns the two named tuples, with the Ontology data.'''
        return self.onto, self.onto_sp

        # delete entries we cannot map to cvterm's
        remove_these = []
        for term in ontology_spanish:
            if not term.VARIABLE_ID_BMS:
                remove_these.append(term)
        for term in remove_these:
            ontology_spanish.remove(term)

        return ontology, ontology_spanish

    def prettyprint_ontology_mapping(method='METHOD_NAME'):
        '''Pretty-prints the ontology mapping.'''
        fmt = "{0:25} -> {1}"
        l = [fmt.format(k,getattr(v[0], method)) for k,v in\
             self.mapping.iteritems()]
        for i in l:
            print l

    def get_translation(self):
        '''Creates a dict() out of the ontologies, mapping Oracle columns to
        chado entitys.'''
        d = dict()
        for k in self.onto_sp:
            d.update({k.SPANISH.upper() : 'phenotype.value'})
        return d

# So we need to join the ontology_spanish.VARIABLE_ID_BMS on
# ontology.VARIABLE_ID and after that we should be able to self join ontology
# on: ontology.TRAIT_ID .SCALE_ID and METHOD_ID

# 'select * from V_ONTOLOGY_SPANISH vos inner join V_ONTOLOGY vo on'\
# +'vos.VARIABLE_ID_BMS = vo.SCALE_ID order by vo.TRAIT_NAME'

# But we did this in python, see CassavaOntology(cursor).mapping
