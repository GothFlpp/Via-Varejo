# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Leitura das origens
df_compra = spark.table("context_site.compra")

df_unidade_negocio = spark.table("context_site.unidadenegocio")

df_freteentregatipo = spark.table("context_site.freteentregatipo")

df_compra_entrega = spark.table("context_site.compraentrega")

df_compra_entrega_status = spark.table("context_site.compraentregastatus")

df_lojista = spark.table("context_site.lojista")

df_compra_entregasku = spark.table("context_site.compraentregasku")

df_sku = spark.table("context_site.sku")

"""print(f"df_compra = {df_compra.count()}")
print(f"df_unidade_negocio = {df_unidade_negocio.count()}")
print(f"df_freteentregatipo = {df_freteentregatipo.count()}")
print(f"df_compra_entrega = {df_compra_entrega.count()}")
print(f"df_compra_entrega_status = {df_compra_entrega_status.count()}")
print(f"df_lojista = {df_lojista.count()}")
print(f"df_compra_entregasku = {df_compra_entregasku.count()}")
print(f"df_sku = {df_sku.count()}")


df_compra = 81.981.350
df_unidade_negocio = 27
df_freteentregatipo = 28
df_compra_entrega = 97.729.351
df_compra_entrega_status = 1.239
df_lojista = 61.522
df_compra_entregasku = 96.938.886
df_sku = 16.850.563

02/09/2020

"""

# COMMAND ----------

# DBTITLE 1,Aplicando regras (Joins, Filters e Alias)
#chaves dos joins
c_u = df_compra["idunidadenegocio"] == df_unidade_negocio["idunidadenegocio"]
c_ce = df_compra["idcompra"] == df_compra_entrega["idcompra"]
ce_fre = df_compra_entrega["idfreteentregatipo"] == df_freteentregatipo["idfreteentregatipo"]
ce_ces = (
  (df_compra_entrega["idcompraentregastatus"] == df_compra_entrega_status["idcompraentregastatus"]) & 
  (df_compra_entrega["bandeira"] == df_compra_entrega_status["bandeira"])
)
ce_l = (
  (df_compra_entrega["idlojista"] == df_lojista["idlojista"]) &
  (df_compra_entrega["bandeira"] == df_lojista["bandeira"])
)
ce_cesku = (
  (df_compra_entrega["idcompraentrega"] == df_compra_entregasku["idcompraentrega"]) & 
  (df_compra_entrega["bandeira"] == df_compra_entregasku["bandeira"])
)
sku_cesku = df_sku["idsku"] == df_compra_entregasku["idsku"]



df_join = (
  df_compra
    .join(broadcast(df_unidade_negocio), c_u, "inner")
    .join(df_compra_entrega, c_ce, "inner")
    .join(broadcast(df_freteentregatipo), ce_fre, "inner")
    .join(broadcast(df_compra_entrega_status), ce_ces, "inner")
    .join(broadcast(df_lojista), ce_l, "inner")
    .join(df_compra_entregasku, ce_cesku, "inner")
    .join(df_sku, sku_cesku, "inner")
    .filter(~df_compra_entrega["idlojista"].isin(15,16,10037))
    .filter(trim(upper(df_compra_entrega["origem"])) == lit("LJ"))
    .filter(df_compra["data"].between("2020-08-01T00:00:00.000+0000", "2020-08-31T00:00:00.000+0000"))
    .select(
      df_compra["data"].alias("DtPedidoVenda"),
      df_compra["dataaprovacao"],
      df_compra["datacriacaoregistro"],
      df_compra["bandeira"],
      df_compra["flagaprovado"],
      df_compra["idcanalvenda"],
      df_compra["idcompra"],
      df_compra["idcliente"],
      df_compra["idlistadecompra"],
      df_unidade_negocio["nome"].alias("NmUnidadeNegocio"),
      df_freteentregatipo["nome"].alias("NmTipoEntrega"),
      df_compra_entrega["idcompraentregastatus"],
      df_compra_entrega_status["nome"].alias("NmSituacaoEntregaSite"),
      df_compra_entrega["dataprevisao"],
      df_compra_entrega["datastatus"],
      df_compra_entrega["dataentrega"],
      df_compra_entrega["gerencialid"],
      df_compra_entrega["dataemissaonotafiscal"],
      df_compra_entrega["dataentregacorrigida"],
      df_compra_entrega["dataprometidaoriginal"],
      df_compra_entrega["chave_nfe"],
      df_lojista["idlojista"],
      df_lojista["nome"].alias("lojista"),
      df_lojista["flagativa"].alias("status_lojista"),
      df_compra_entrega["datalimitesaidacd"],
      df_compra_entrega["dataentregaajustada"],
      df_compra_entregasku["idsku"],
      df_sku["nome"].alias("descricao_sku"),
      df_compra_entregasku["valorvendaunidade"],
      df_compra_entregasku["valorvendaunidademenoscupomnominal"],
      df_compra_entregasku["valorfretecomdesconto"],
      year(df_compra["data"]).alias("year_partition"),
      month(df_compra["data"]).alias("month_partition"),
      dayofmonth(df_compra["data"]).alias("day_partition")      
    )
)


# COMMAND ----------

# DBTITLE 1,Salvando resultado em Delta Table
path = "/mnt/gen2/app/marketplace/visao_geral/"
name = "app_marketplace.visao_geral"
(df_join
   .write
   .format("delta")
   .mode("overwrite")
   .option("mergeSchema", "true")
   .partitionBy("year_partition", "month_partition", "day_partition")
   .saveAsTable(name = name, path = path)
)

