'''
Cassava Phenotyping Ontology

We need to know this for translating the columns to their chado equivalent.
'''

from collections import namedtuple
import utility

def get_tabledata_as_tuple(cursor, table):
    '''Create a list of namedtuple's for <table> with <column_names> as
    members.
    
    Arguments:
        cursor      a PEP 249 compliant cursor pointing to the oracledb
        table       name of the table
    '''
    sql = utility.OracleSQLQueries.get_column_metadata_from
    sql = sql.format(table_name=table)
    cursor.execute(sql)
    column_names = [line[1] for line in cursor.fetchall()]

    VOntology = namedtuple('VOntology', column_names)
    sql = utility.OracleSQLQueries.get_all_from
    sql = sql.format(table=table)
    cursor.execute(sql)
    vonto = [VOntology(*line) for line in cursor.fetchall()]

    return vonto

class CassavaOntology():
    '''Maybe self-explanatory.'''

    SPANISH_ONTOLOGY_TABLE = 'V_ONTOLOGY_SPANISH'
    ONTOLOGY_TABLE = 'V_ONTOLOGY'

    def __init__(self, cursor):
        '''We need that cursor.'''
        self.c = cursor
        self.onto, self.onto_sp = self.get_ontologies()

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
        ontology_spanish = get_tabledata_as_tuple(self.c,
            self.SPANISH_ONTOLOGY_TABLE)
        ontology = get_tabledata_as_tuple(self.c, self.ONTOLOGY_TABLE)

        # delete entries we cannot map to cvterm's
        remove_these = []
        for term in ontology_spanish:
            if not term.VARIABLE_ID_BMS:
                remove_these.append(term)
        for term in remove_these:
            ontology_spanish.remove(term)

        return ontology, ontology_spanish

# So we need to join the ontology_spanish.VARIABLE_ID_BMS on
# ontology.VARIABLE_ID and after that we should be able to self join ontology
# on: ontology.TRAIT_ID .SCALE_ID and METHOD_ID

# 'select * from V_ONTOLOGY_SPANISH vos inner join V_ONTOLOGY vo on'\
# +'vos.VARIABLE_ID_BMS = vo.SCALE_ID order by vo.TRAIT_NAME'

# Buts lets do this in python... #TODO
