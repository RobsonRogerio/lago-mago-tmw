# Databricks notebook source
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()
    
# tablename = dbutils.widgets.get('tablename')
tablename = 'cliente'

query = import_query(f'{tablename}.sql')

# spark.sql(query) \
#     .write \
#     .format('delta') \
#     .mode('overwrite') \
#     .option('overwriteSchema', 'true') \
#     .saveAsTable(f'silver.upsell.{tablename}')

# COMMAND ----------

# DBTITLE 1,Leitura do CDF da tabela delta para um df
df = spark.readStream \
        .format('delta') \
        .option('readChangeFeed', 'true') \
        .table(f'bronze.upsell.customers')

# COMMAND ----------

def upsert(df, BatchId=None):
    try:
        df.createOrReplaceTempView("cdf_temp_view")

        silver = 'silver.upsell.cliente'

        merge_sql = f"""
        MERGE INTO {silver} AS s
        USING (
            SELECT *
            FROM cdf_temp_view
            WHERE _change_type <> 'update_preimage'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY _commit_timestamp DESC) = 1
        ) AS d
        ON s.IdCliente = d.IdCliente
        WHEN MATCHED AND d._change_type = 'delete' THEN DELETE
        WHEN MATCHED AND d._change_type = 'update_postimage' THEN UPDATE SET *
        WHEN NOT MATCHED AND (d._change_type = 'insert' OR d._change_type = 'update_postimage') THEN INSERT *
        """

        spark.sql(merge_sql)
    except Exception as e:
        print(f'Erro no upsert do batch {BatchId}: {e}')
        raise e

stream = df.writeStream \
    .option("checkpointLocation", f"/Volumes/raw/upsell/cdc/silver_{tablename}_checkpoints") \
    .foreachBatch(lambda batch_df, epoch: upsert(batch_df)) \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# # Exemplo com DeltaTable.merge()

# from delta.tables import DeltaTable
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# # Nome da tabela destino (silver)
# silver_table = f"silver.upsell.{tablename}"
# silver = DeltaTable.forName(spark, silver_table)

# def upsert(df, BatchId=None):
#     # Cria janela para pegar o último registro por IdCliente
#     window = Window.partitionBy("IdCliente").orderBy(F.col("_commit_timestamp").desc())

#     # Remove update_preimage e mantém só o último registro por cliente
#     df_last = (
#         df.filter(F.col("_change_type") != "update_preimage")
#           .withColumn("rn", F.row_number().over(window))
#           .filter(F.col("rn") == 1)
#           .drop("rn")
#     )

#     # Seleciona apenas colunas de negócio (ignora metadados do CDF)
#     exclude_cols = ["_change_type", "_commit_timestamp", "_commit_version"]
#     cols_to_update = [c for c in df_last.columns if c not in exclude_cols]

#     # Expressões dinâmicas para update e insert
#     update_expr = {c: F.col(f"d.{c}") for c in cols_to_update}
#     insert_expr = {c: F.col(f"d.{c}") for c in cols_to_update}

#     # Aplica merge na Silver
#     silver.alias("s") \
#         .merge(df_last.alias("d"), "s.IdCliente = d.IdCliente") \
#         .whenMatchedDelete(condition="d._change_type = 'delete'") \
#         .whenMatchedUpdate(set=update_expr, condition="d._change_type = 'update_postimage'") \
#         .whenNotMatchedInsert(values=insert_expr, condition="d._change_type IN ('insert','update_postimage')") \
#         .execute()

# # Inicia o stream aplicando o upsert
# stream = df.writeStream \
#     .option("checkpointLocation", f"/Volumes/raw/upsell/cdc/silver_{tablename}_checkpoints") \
#     .foreachBatch(upsert) \
#     .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.upsell.customers
# MAGIC union all
# MAGIC select count(*) from silver.upsell.cliente
