# Databricks notebook source
from delta.tables import delta

# COMMAND ----------

def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()
    
tablename = dbutils.widgets.get('tablename')

query = import_query(f'{tablename}.sql')

spark.sql(query) \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'silver.upsell.{tablename}')

# COMMAND ----------

df = spark.readStream \
        .format('delta') \
        .option('readChangeFeed', 'true') \
        .table(f'bronze.upsell.customers')

# COMMAND ----------

silver = delta.table(f'silver.upsell.{tablename}')

def upsert(df, deltatable):
    query_last = """
        SELECT *
        FROM {df}
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {id} ORDER BY _commit_timestamp DESC)
        """
    df_last = spark.sql(query_last, df=df, id='idCliente')
    
    pass

stream = df.writeStream \
            .option("checkpointLocation", f"/Volumes/raw/upsell/cdc/silver_{tablename}_checkpoints") \
            .foreachBatch(lambda df, epoch: df.write.format('delta').mode('append').saveAsTable(f'silver.upsell.{tablename}')) \
            .start()
