# Databricks notebook source
# MAGIC %sql
# MAGIC select * from table_changes('bronze.upsell.customers', 1, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('bronze.upsell.customers', 1, 2) WHERE IdCliente = 'd7f7eb75-4f0b-4d15-960d-8efe5ca7dd7e';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('bronze.upsell.transactions_product', 1, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('bronze.upsell.products', 1, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('bronze.upsell.transactions', 1, 2)
