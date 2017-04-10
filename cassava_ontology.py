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
    column_names.reverse()

    VOntology = namedtuple('VOntology', column_names)
    sql = utility.OracleSQLQueries.get_all_from
    sql = sql.format(table=table)
    cursor.execute(sql)
    vonto = [VOntology(*line) for line in cursor.fetchall()]

    return vonto

# So we need to join the ontology_spanish on ontology.VARIABLE_ID
# And after that we should be able to self join ontology on:
#   ontology.TRAIT_ID .SCALE_ID and METHOD_ID

# 'select * from V_ONTOLOGY_SPANISH vos inner join V_ONTOLOGY vo on'\
# +'vos.VARIABLE_ID_BMS = vo.SCALE_ID order by vo.TRAIT_NAME'

# Buts lets do this in python... #TODO
