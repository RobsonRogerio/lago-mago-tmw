import delta
import utils

class ingestor:

    def __init__(self, spark, catalog, schemaname, tablename, data_format):
        self.spark = spark
        self.catalog = catalog
        self.schemaname = schemaname
        self.tablename = tablename
        self.format = data_format
        self.set_schema()

    def set_schema(self):
        # Importa o schema definido para a tabela
        self.data_schema = utils.import_schema(self.tablename)
    
    def load(self, path):
        # Lê os dados do caminho informado usando o schema definido
        df = (
            self.spark.read
            .format(self.format)
            .schema(self.data_schema)
            .load(path)
        )
        return df

    def save(self, df):
        table_full_name = f"{self.catalog}.{self.schemaname}.{self.tablename}"

        # Salva a tabela diretamente no metastore Delta
        df.write.format("delta").mode("overwrite").saveAsTable(table_full_name)

        # Habilita Change Data Feed (CDF) imediatamente após a criação
        self.spark.sql(f"""
            ALTER TABLE {table_full_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)

        return True
        
    def execute(self, path):
        df = self.load(path)
        return self.save(df)
    
class ingestorCDC(ingestor):

    def __init__(self, spark, catalog, schemaname, tablename, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, schemaname, tablename, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()
        
    def set_deltatable(self):
        tablename = f"{self.catalog}.{self.schemaname}.{self.tablename}"
        self.deltatable = delta.DeltaTable.forName(self.spark, tablename)

    def upsert(self, df, BatchId=None):
        df.createOrReplaceTempView("cdc_temp_view")

        table_full_name = f"{self.catalog}.{self.schemaname}.{self.tablename}"
        merge_sql = f"""
        MERGE INTO {table_full_name} AS b
        USING (
            SELECT * FROM cdc_temp_view
            QUALIFY row_number() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
        ) AS d
        ON b.{self.id_field} = d.{self.id_field}
        WHEN MATCHED AND d.op = 'D' THEN DELETE
        WHEN MATCHED AND d.op = 'U' THEN UPDATE SET *
        WHEN NOT MATCHED AND (d.op = 'I' OR d.op = 'U') THEN INSERT *
        """

        self.spark.sql(merge_sql)

    def load(self, path):
        df = self.spark\
                    .readStream \
                    .format("cloudFiles") \
                    .option("cloudFiles.format", self.format) \
                    .schema(self.data_schema) \
                    .load(path)
        return df
    
    def save(self, df):
        stream = df.writeStream \
                    .option("checkpointLocation", f"/Volumes/raw/{self.schemaname}/cdc/{self.tablename}_checkpoints") \
                    .foreachBatch(lambda df, _: self.upsert(df)) \
                    .trigger(availableNow=True)
        return stream.start()
