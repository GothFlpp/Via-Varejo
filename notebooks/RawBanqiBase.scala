// Databricks notebook source
// DBTITLE 1,Imports
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.time.Instant
import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
import org.apache.spark.sql.functions.{array_remove, col, concat, from_json, lit, regexp_replace, split, trim, expr, size, explode_outer, lpad, date_add, dayofmonth, year, month, hour, when, element_at, struct, collect_list, typedLit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, DataFrame, Column}
import spark.implicits._
import scala.collection.mutable.ArrayBuffer

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.vvprdstgdatalake.dfs.core.windows.net",
  dbutils.secrets.get(scope = "vvprdstgdatalake", key = "account_key1"))

dbutils.fs.ls("abfss://raw@vvprdstgdatalake.dfs.core.windows.net/site/compra")

// COMMAND ----------

// DBTITLE 1,Header Class
//Classe criada no processo de validacao
case class Validation(
    isNotOk : Boolean,
    log : String
  )

//Classe contendo todos os metodos utilizados no processo
case class IngestaoAzure(
    origem : String,
    tabela : String,
    modo_gravacao : String,
    caminho : String,
    structType : String,
    chaves : Array[String]) {
    //Método que cria StrucType dinamicamente
    def getConteudoStructType(): StructType = {DataType.fromJson(this.structType).asInstanceOf[StructType]}
  
    //Metodo que retorna LOCATION da tabela
    def getRawSaveLocation() : String = {
        "/mnt/gen2"+ this.caminho + "/" + this.tabela}
  
    //Metodo que retorna DATABASE e NOME da tabela
    def getRawTableName() : String = {
        "raw_"+this.origem+"."+this.tabela}
  
    //Metodo que valida se registros vieram zerados
    def isEmptyRaw(df : DataFrame) : Unit = {
        //saida para header quantidade 0 com processamento OK
        if(df.limit(3).rdd.isEmpty)
            {dbutils.notebook.exit("# Header da tabela com quantidade 0")}
        else
            {println("Counteudo contem registros")}}
  
    //Método que valida se colunas chaves estao nulas
    def validateTableKeyColumns (df:Dataset[Row]) : Validation = {
        val tableKeys = this.chaves
        var hasNulls = false
        var message = "Inicio da validacao de valores nulos nas colunas chave da tabela"
        for (key <- tableKeys) {
            val numberOfNullValues = df.filter(col(key).isNull).count()
            if(numberOfNullValues>0){
                hasNulls = true
                message= message.concat("""
                | ERRO : Coluna chave """ + key)}
            else{
                message= message.concat(""" 
                | OK : Coluna chave """ + key)}
        }
        if(tableKeys.length == 0){
            message= message.concat(""" 
            | OK : Tabela não possui chaves """)}
        Validation(hasNulls, message)}
   
 
    //Método que converte todos os campos do Struct para String
    def CreateStringSchema(schema : StructType) : StructType = {
        println("Criando novo schema com todos os campos convertidos para String")
        val rdd = spark.sparkContext.emptyRDD[Row]
        var df = spark.createDataFrame(rdd, schema)
        for (row <- schema.fields){
          if (row.dataType.toString != "StringType"){
            println(s"FIELD : ${row.name} | TYPE: ${row.dataType} | ACTION : Cast to String")
            df = df.withColumn(row.name, col(row.name).cast(StringType))}
         }
        df.schema
    }

    //Metodo que converte dataTypes do DF para o schema original
    def castFromString(df : Dataset[Row], schema: StructType) : DataFrame = {
      println("Convertendo campos diferentes de String para seu DataType original")
      var dfClean = df
      val schema_original = schema
      for (row <- schema_original.fields){
          if (row.dataType.toString != "StringType"){
              println(s"FIELD : ${row.name} | TYPE: ${row.dataType} ACTION : Cast to ${row.dataType}")
              dfClean = dfClean.withColumn(row.name, col(row.name).cast(row.dataType))}
        }
      dfClean}
  
    //Método que cria tabela FINAL vazia
    def createTableRaw(df : DataFrame, formato : String, savemode: String) : Unit = {
        //cria database caso nao exista
        val createDatabase = s"""CREATE DATABASE if not exists raw_${this.origem}"""
        println(createDatabase)
        spark.sql(createDatabase)
        df.limit(0).coalesce(1)
            .write.format(formato)
            .partitionBy("year_partition", "month_partition", "day_partition")
            .mode(savemode)
            .option("path", this.getRawSaveLocation)
            .saveAsTable(this.getRawTableName)}
  
    //Metodo que faz SAVE na LOCATION da tabela final
    //Metodo que faz SAVE na LOCATION da tabela final
    def saveRaw(df : DataFrame, formato : String, partitions : Array[Any], modo_gravacao : String = this.modo_gravacao) : Unit = {
        println(s"Salvando DF final na LOCATION : ${this.getRawSaveLocation} no MODE : ${modo_gravacao}")
        df.repartition(200)
            .write.format(formato)
            .partitionBy("year_partition", "month_partition", "day_partition")
            .mode(modo_gravacao)
            .option("replaceWhere", 
                    s"""year_partition == '${partitions(0)}' AND 
                      month_partition == '${partitions(1)}' AND 
                      day_partition == '${partitions(2)}'""")
            .save(this.getRawSaveLocation)
         println(s"SAIDA SALVA")}
  
}



// COMMAND ----------

// DBTITLE 1,Header Object
//Object da case class IngestaoAzure   
object IngestaoAzure {
      //Metodo que cria schema utilizado para transformar header vindo do arquivo
      def getHeaderSchema(): StructType = {
          val schemaOrigem = new StructType().add("tipo", StringType)
              .add("fonte", StringType)
              .add("caminho", StringType)
              .add("diretorio", StringType)

          val schemaDestino = (new StructType().add("tipo", StringType)
              .add("caminho", StringType)
              .add("diretorio", StringType)
              .add("chave_tabela", StringType)
              .add("modo_gravacao", StringType)
              .add("structType", StringType)
              .add("quantidade", IntegerType))

          val schema = new StructType()
              .add("_id", StringType)
              .add("versao", StringType)
              .add("unidade_negocio", StringType)
              .add("projeto", StringType)
              .add("descricao", StringType)
              .add("timestamp", StringType)
              .add("origem", schemaOrigem)
              .add("destino", schemaDestino)
              .add("conteudo", StringType)
              .add("lote", StringType)
          schema}
  
    //Metodo que corrige structType para que o schema possa ser criado
     def correctStructType (df : Dataset[Row]) : Dataset[Row] = {
         val dfCorrigido = df
             .withColumn("structType", concat(lit("{\"type\":\"struct\","), expr("substring(structType, 2, length(structType)-1)")))
             .withColumn("structType", regexp_replace($"structType", "dataType", "type"))
         dfCorrigido}
    
    //Metodo que recebe DataFrame e cria Dataset do tipo IngestaoAzure
     def createHeaderObject(df : Dataset[Row]) : IngestaoAzure = {
         var dfTemp = df.limit(1)
          
         dfTemp = dfTemp.select($"origem.fonte".as("origem"),
             $"destino.diretorio".as("tabela"),
             $"destino.modo_gravacao".as("modo_gravacao"),
             $"destino.caminho".as("caminho"),
             $"destino.structType".as("structType"),
             array_remove(split(regexp_replace($"destino.chave_tabela"," ",""),","),"").as("chaves"))          
         dfTemp = correctStructType(dfTemp)
         dfTemp.as[IngestaoAzure].take(1)(0)}
    
  }


// COMMAND ----------

//Pegando data atual do processamento
val data_proc = dbutils.widgets.get("data_proc")
//transformando data atual do processamento para segundos
val data_proc_epoch = (DateTime.parse(data_proc, DateTimeFormat.forPattern("yyyy-MM-dd")).getMillis / 1000)
//subtraindo 1 dia da data do processamento em segundos
val data_proc_epoch_d1 = data_proc_epoch - 86400

//transforma data em timestamp
//data inicio que sera trazida do eventhub
val inicio = Instant.ofEpochSecond(data_proc_epoch_d1)
//data final que sera trazida do eventhub
val fim = Instant.ofEpochSecond(data_proc_epoch)

//Connection EventHub
val ehConf = EventHubsConf("Endpoint=sb://dlprdevhstreamingbanqidev.servicebus.windows.net/;SharedAccessKeyName=Listen;SharedAccessKey=d1+jFGbHpvj86mQMwVNluZSaPq9Cxt0hlzldiYTtUaY=;EntityPath=banqidev")
  .setStartingPosition(EventPosition.fromEnqueuedTime(inicio))
  .setEndingPosition(EventPosition.fromEnqueuedTime(fim))

//Lê dados do eventHub D-1 em formato batch
val df_read_hub = spark
  .read
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

//Converte campo "Body" de binario para String
val df_hub = df_read_hub.withColumn("Body", col("Body").cast("string"))


// COMMAND ----------

// DBTITLE 1,limpa diretório
dbutils.fs.rm("/mnt/blob/prd-processing/eventhub/raw/banqi/bruto/", true)

// COMMAND ----------

//Instancia objeto schemaheader
val schemaHeader =  IngestaoAzure.getHeaderSchema

//extrai campo Body
val df_body = df_hub
    .select(from_json(col("Body"), schemaHeader).as("values"))
    .select("values.*")
    .filter(col("destino.quantidade") =!= lit(0))
//Coloca df_body em cache uma vez que sera usado N vezes por processo

val path_hub = s"/mnt/blob/prd-processing/eventhub/raw/banqi/bruto/${data_proc}"

df_body.withColumn("header_destino_diretorio", $"destino.diretorio")
  .write
  .format("json")
  .mode("overwrite")
  .partitionBy("header_destino_diretorio")
  .save(path_hub)
