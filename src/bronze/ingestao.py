
# COMMAND ----------

# DBTITLE 1,Imports
import delta
import sys

sys.path.insert(0, '../lib')

import utils
import ingestors
from pyspark.sql.functions import col, to_timestamp

# COMMAND ----------

# DBTITLE 1,Setup

catalog = 'bronze'
schemaname = 'upsell'
tablename = dbutils.widgets.get('tablename')
id_field = dbutils.widgets.get('id_field')
timestamp_field = dbutils.widgets.get('timestamp_field')

full_load_path = f"/Volumes/raw/{schemaname}/full_load/{tablename}/"
cdc_path = f"/Volumes/raw/{schemaname}/cdc/{tablename}/"
checkpoint_location = f"/Volumes/raw/{schemaname}/cdc/{tablename}_checkpoints"

##Usar como testes e exemplos
# tablename = 'transactions'
# id_field = 'IdTransacao'
# timestamp_field = 'change_timestamp'

# COMMAND ----------

# DBTITLE 1,Ingestão do Full Load
if not utils.table_exists(spark, catalog, schemaname, tablename):

    print('Tabela não existe, criando...')

    dbutils.fs.rm(checkpoint_location, True)
    
    ingest_full_load = ingestors.ingestor(spark=spark, 
                                        catalog=catalog,
                                        schemaname=schemaname, 
                                        tablename=tablename, 
                                        data_format='parquet')
    ingest_full_load.execute(full_load_path)

    print(f'Criando tabela {tablename} em {catalog}.{schemaname}. Tabela criada com sucesso!')

else:
    print(f'Tabela {tablename} já existente em {catalog}.{schemaname}, ignorando full-load')

# COMMAND ----------

ingest_cdc = ingestors.ingestorCDC(spark=spark,
                                  catalog=catalog,
                                  schemaname=schemaname, 
                                  tablename=tablename, 
                                  data_format='parquet',
                                  id_field=id_field,
                                  timestamp_field=timestamp_field)

stream = ingest_cdc.execute(cdc_path)

# COMMAND ----------

# DBTITLE 1,Leitura do CDC
# from pyspark.sql.window import Window
# from pyspark.sql import functions as F
# data_schema = utils.import_schema(tablename)
# bronze = delta.DeltaTable.forName(spark, f'{catalog}.{schemaname}.{tablename}')

# def upsert(df, table_name):
#     df.createOrReplaceTempView("cdc_temp_view")

#     merge_sql = f"""
#     MERGE INTO {tablename} AS b
#     USING (
#         SELECT * FROM cdc_temp_view
#         QUALIFY row_number() OVER (PARTITION BY {id_field} ORDER BY {timestamp_field} DESC) = 1
#     ) AS d
#     ON b.{id_field} = d.{id_field}
#     WHEN MATCHED AND d.op = 'D' THEN DELETE
#     WHEN MATCHED AND d.op = 'U' THEN UPDATE SET *
#     WHEN NOT MATCHED AND (d.op = 'I' OR d.op = 'U') THEN INSERT *
#     """

#     spark.sql(merge_sql)


# df_stream = spark.readStream \
#       .format("cloudFiles") \
#       .option("cloudFiles.format", "parquet") \
#       .schema(data_schema) \
#       .load(f"/Volumes/raw/data/cdc/{tablename}/")

# stream = df_stream.writeStream \
#       .option("checkpointLocation", f"/Volumes/raw/data/cdc/{tablename}_checkpoints") \
#       .foreachBatch(lambda df, BatchId: upsert(df, f'{catalog}.{schemaname}.{tablename}')) \
#       .trigger(availableNow=True)

# COMMAND ----------

# DBTITLE 1,Leitura do CDC sugerida pelo Assistant
# Opção sugerida pelo Assistant do Databricks:
# You need to specify the cloudFiles.schemaLocation option to enable schema inference and evolution when using Auto Loader. This option points to a directory where Databricks will store schema information for your stream. Here is the fixed code:

# df_stream = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "parquet")
#     .option("cloudFiles.schemaLocation", f"/Volumes/raw/data/cdc/{tablename}/_schemas")
#     .load(f"/Volumes/raw/data/cdc/{tablename}/")
# )

# This code adds the required schemaLocation option. The directory specified will track schema changes over time for your streaming source.