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

# from pyspark.sql import SparkSession

# # automatizar o mapeamento das colunas do CDF para a Silver lendo o schema
# # da tabela Silver e renomeando automaticamente as colunas do DataFrame do 
# # CDF que tiverem correspondência parcial.

# def auto_rename_columns(df, silver_table):
#     """
#     Renomeia automaticamente as colunas do DataFrame df para bater com as colunas da tabela Silver.
#     - df: DataFrame do CDF
#     - silver_table: nome completo da Silver, ex: "silver.upsell.cliente"
#     """
#     # Pega as colunas da Silver
#     silver_cols = [row['col_name'] for row in spark.sql(f"DESCRIBE TABLE {silver_table}").collect()]

#     # Cria dicionário de renomeação: CDF -> Silver
#     rename_map = {}
#     for s_col in silver_cols:
#         # procura correspondência ignorando case e underscores
#         for c_col in df.columns:
#             if c_col.lower().replace("_","") == s_col.lower().replace("_",""):
#                 rename_map[c_col] = s_col
#                 break

#     # Aplica o rename
#     for old, new in rename_map.items():
#         df = df.withColumnRenamed(old, new)

#     return df

# COMMAND ----------

def upsert(df, BatchId=None):
    try:
        silver = "silver.upsell.cliente"

        # Seleciona apenas as colunas que existem na Silver
        df = df.select(
            "IdCliente",
            "QtdePontos",  # ou "nrPontosCliente" se já tiver renomeado
            "FlEmail",     # ou "flEmailCliente"
            "_change_type",
            "_commit_timestamp"
        )

        # Opcional: renomear manualmente as colunas do CDF para bater com a Silver
        df = df.withColumnRenamed("IdCliente", "idCliente") \
               .withColumnRenamed("QtdePontos", "nrPontosCliente") \
               .withColumnRenamed("FlEmail", "flEmailCliente")

        # Debug rápido
        df.show(5)
        df.printSchema()

        df.createOrReplaceTempView("cdf_temp_view")

        # Recupera todas as colunas do DataFrame
        exclude_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
        cols = [c for c in df.columns if c not in exclude_cols]

        # Gera expressões dinâmicas para update e insert
        update_expr = ", ".join([f"s.{c} = d.{c}" for c in cols])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"d.{c}" for c in cols])

        merge_sql = f"""
        MERGE INTO {silver} AS s
        USING (
            SELECT *
            FROM cdf_temp_view
            WHERE _change_type <> 'update_preimage'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY idCliente ORDER BY _commit_timestamp DESC) = 1
        ) AS d
        ON s.idCliente = d.idCliente
        WHEN MATCHED AND d._change_type = 'delete' THEN DELETE
        WHEN MATCHED AND d._change_type = 'update_postimage' THEN 
            UPDATE SET 
                s.nrPontosCliente = d.nrPontosCliente,
                s.flEmailCliente = d.flEmailCliente
        WHEN NOT MATCHED AND d._change_type IN ('insert','update_postimage') THEN 
            INSERT (idCliente, nrPontosCliente, flEmailCliente) 
            VALUES (d.idCliente, d.nrPontosCliente, d.flEmailCliente)
        """

        spark.sql(merge_sql)

    except Exception as e:
        print(f"Erro no upsert do batch {BatchId}: {e}")
        raise e

stream = df.writeStream \
.option("checkpointLocation", f"/Volumes/raw/upsell/cdc/silver_{tablename}_checkpoints") \
.foreachBatch(lambda batch_df, epoch: upsert(batch_df)) \
.trigger(availableNow=True) \
.start()


stream.awaitTermination()

# COMMAND ----------

stream.isActive  # True se ainda estiver rodando

# COMMAND ----------

stream.stop()

# COMMAND ----------

# Exemplo com DeltaTable.merge()

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Nome da tabela destino (silver)
silver_table = f"silver.upsell.{tablename}"
silver = DeltaTable.forName(spark, silver_table)

def upsert(df, BatchId=None):
    # Cria janela para pegar o último registro por IdCliente
    window = Window.partitionBy("IdCliente").orderBy(F.col("_commit_timestamp").desc())

    # Remove update_preimage e mantém só o último registro por cliente
    df_last = (
        df.filter(F.col("_change_type") != "update_preimage")
          .withColumn("rn", F.row_number().over(window))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

    # Seleciona apenas colunas de negócio (ignora metadados do CDF)
    exclude_cols = ["_change_type", "_commit_timestamp", "_commit_version"]
    cols_to_update = [c for c in df_last.columns if c not in exclude_cols]

    # Expressões dinâmicas para update e insert
    update_expr = {c: F.col(f"d.{c}") for c in cols_to_update}
    insert_expr = {c: F.col(f"d.{c}") for c in cols_to_update}

    # Aplica merge na Silver
    silver.alias("s") \
        .merge(df_last.alias("d"), "s.IdCliente = d.IdCliente") \
        .whenMatchedDelete(condition="d._change_type = 'delete'") \
        .whenMatchedUpdate(set=update_expr, condition="d._change_type = 'update_postimage'") \
        .whenNotMatchedInsert(values=insert_expr, condition="d._change_type IN ('insert','update_postimage')") \
        .execute()

# Inicia o stream aplicando o upsert
stream = df.writeStream \
    .option("checkpointLocation", f"/Volumes/raw/upsell/cdc/silver_{tablename}_checkpoints") \
    .foreachBatch(upsert) \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.upsell.customers
# MAGIC union all
# MAGIC select count(*) from silver.upsell.cliente
