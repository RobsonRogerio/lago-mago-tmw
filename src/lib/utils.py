import json
from pyspark.sql import types

# Outra abordagem para a função table_exists usando DESCRIBE TABLE com Try/Except (mais robusto)
def table_exists(spark, catalog, database, table):
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{database}.{table}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

# Vantagem: evita depender do formato do SHOW TABLES e do preenchimento da coluna database.
# Funciona mesmo se a tabela for externa, view ou Delta table no Unity Catalog.

def import_schema(tablename):
    path_json = '/Workspace/Users/robson.rogerio@gmail.com/lago-mago-tmw/src/bronze/'
    with open(f'{path_json}{tablename}.json', 'r') as open_file:
        schema_json = json.load(open_file) ## retorna um dicionario
    schema_df = types.StructType.fromJson(schema_json)
    return schema_df