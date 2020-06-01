# Databricks notebook source
origem="site"
tabela=("compraentregasku").lower()

# COMMAND ----------

spark.sql("show create table raw_"+origem+"."+tabela).show(1, False)

# COMMAND ----------

#Somente bandeira_partition
#spark.sql("CONVERT TO DELTA parquet.`/mnt/gen2/raw/"+origem+"/"+tabela+"` PARTITIONED BY (bandeira_partition string)")

Bandeira e Date partition
spark.sql("CONVERT TO DELTA parquet.`/mnt/gen2/raw/"+origem+"/"+tabela+"` PARTITIONED BY (bandeira_partition string, year_partition string, month_partition string, day_partition string)")

# COMMAND ----------

spark.sql("drop table raw_"+origem+"."+tabela)

# COMMAND ----------

spark.sql("CREATE TABLE raw_"+origem+"."+tabela+" USING DELTA LOCATION '/mnt/gen2/raw/"+origem+"/"+tabela+"'")

# COMMAND ----------

spark.sql("ALTER TABLE raw_"+origem+"."+tabela+" SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)")

# COMMAND ----------

display(spark.sql("select * from raw_"+origem+"."+tabela))

# COMMAND ----------

spark.sql("show create table raw_"+origem+"."+tabela).show(1, False)
