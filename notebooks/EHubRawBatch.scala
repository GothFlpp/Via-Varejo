// Databricks notebook source
// DBTITLE 1,Imports
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.time.Instant
import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
import org.apache.spark.sql.functions.{array_remove, col, concat, from_json, lit, regexp_replace, split, trim, expr, size, explode_outer, date_add, dayofmonth, year, month}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, DataFrame}
import spark.implicits._
import scala.collection.mutable.ArrayBuffer

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.vvprdstgdatalake.blob.core.windows.net",
  "p4iPN0I0EsYWOSi3AXBKwP19Blf7ZnnKhMjxxfVooELwWkP+Q5ni9s38Q3/AcsauEaDzLd42m9/TysEXQfMrfw==")

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
        "/Felippe/eventhub"+ this.caminho + "/" + this.tabela}
  
    //Metodo que retorna DATABASE e NOME da tabela
    def getRawTableName() : String = {
        "teste_raw_"+this.origem+"."+this.tabela}
  
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
        val createDatabase = s"""CREATE DATABASE if not exists teste_raw_${this.origem}"""
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

// DBTITLE 1,Read EventHub D-1
//Pegando data atual do processamento
val data_proc = dbutils.widgets.get("data_proc")
val modo = dbutils.widgets.get("modo").toLowerCase()
//val tabela = dbutils.widgets.get("tabela").toLowerCase()

//criando particoes
//formato de data passado pelo Data Factory
val dateFormat = "yyyy-MM-dd"
val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
//convertendo data no formato string para formato date e subtraindo 1 dia
val d = java.time.LocalDate.parse(data_proc, dtf).plusDays(-1)
//extraindo ano
val year_partition = d.getYear
//extraind mes
val month_partition = d.getMonthValue
//extraind dia
val day_partition = d.getDayOfMonth
//transformando data atual do processamento para segundos
val data_proc_epoch = (DateTime.parse(data_proc, DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()).getMillis / 1000)
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

//Salva dados em diretório temporário
//val path_hub = "wasbs://dev@vvprdstgdatalake.blob.core.windows.net/Felippe/eventhub/raw/banqi/bruto/"
//df_hub.write.format("json").mode("overwrite").save(path_hub)

//df_hub.count()

// COMMAND ----------

// DBTITLE 1,Extract Body
//Instancia objeto schemaheader
val schemaHeader =  IngestaoAzure.getHeaderSchema

//Lê arquivos do eventHub
//val df_hub = spark.read.format("json").load(path_hub)

//extrai campo Body
val df_body = df_hub
    .select(from_json(col("Body"), schemaHeader).as("values"))
    .select("values.*")
    .filter(col("destino.quantidade") =!= lit(0))
//Coloca df_body em cache uma vez que sera usado N vezes por processo
df_body.cache().count()

// COMMAND ----------

// DBTITLE 1,Função que extrai Conteúdo
//funcao que extrai conteudo por tabela e cria campos de particao
def extractConteudo(df : DataFrame, table : String) : Unit = {
    println(s"filtrando DF apenas por table ${table}")
    val dfLanding = df.filter($"destino.diretorio" === lit(table))
    println("""Removendo [] do campo conteudo""")
    val dfLandingTrim = dfLanding.withColumn("conteudo", trim(col("conteudo"), "[]"))
  
    println("Instancia objeto messageHeader")
    val messageHeader = IngestaoAzure.createHeaderObject(dfLandingTrim)

    println("Verifica se DF contem dados")
    messageHeader.isEmptyRaw(dfLandingTrim)

    println("Cria structType atraves do campo fields")
    val schema = messageHeader.getConteudoStructType

    println("Converte todos os campos do schema para string")
    val string_schema = messageHeader.CreateStringSchema(schema)

    println("Explode campos do conteudo com schema em String")
    val dfFormat = dfLandingTrim.withColumn("conteudo", from_json($"conteudo",string_schema))

    println("Seleciona todos os campos necessarios do conteudo e renomeia os campos anteriores")
    var dfClean = dfFormat
        .select(
            $"_id".as("header_id"),
            $"versao".as("header_versao"),
            $"unidade_negocio".as("header_unidade_negocio"),
            $"projeto".as("header_projeto"),
            $"descricao".as("header_descricao"),
            $"timestamp".as("header_timestamp"),
            $"origem".as("header_origem"),
            $"destino".as("header_destino"),
            $"lote".as("header_lote"),
            $"conteudo.*")

    println("Retorna campos do conteudo para dataType original apos a explosao")
    dfClean = messageHeader.castFromString(dfClean, schema)

    println("Adiciona dados de particao")
    val dfFinal = dfClean
        .withColumn("year_partition", lit(year_partition))
        .withColumn("month_partition", lit(month_partition))
        .withColumn("day_partition", lit(day_partition))
  
    println("booleano que retorna true caso colunas chaves estejam OK")
    val validationKeys = messageHeader.validateTableKeyColumns(dfFinal)

    println("condicao que forca erro caso colunas chaves estejam vazias ou nao existam")
    if(validationKeys.isNotOk){
        println(validationKeys.log)
        System.exit(1)}
    
    println(s" Salvando registros na TABELA : ${messageHeader.getRawTableName}")
    println(s" LOCATION : ${messageHeader.getRawSaveLocation}")
    println(s" DATA DE PROCESSAMENTO : ${data_proc}")
    //booleano que retorna true caso tabela exista
    val tableExists = spark.catalog.tableExists(messageHeader.getRawTableName)

    //Salvando particoes em um array
    val partitions : Array[Any] = Array(year_partition, month_partition, day_partition)

    val modo_gravacao = dbutils.widgets.get("modo_gravacao").toLowerCase()


    //condicao que verifica se tabela destino existe
    //caso exista : realiza save
    //caso nao exista : realiza create table e save
    if(!tableExists){
        println("Criando Tabela")
        messageHeader.createTableRaw(dfFinal, modo, "append")
        println("Executando Save")
        if (modo_gravacao != "overwrite"){messageHeader.saveRaw(dfFinal, modo, partitions)}
        else{messageHeader.saveRaw(dfFinal, modo, partitions, modo_gravacao)}
    }
    else{
        print("A Tabela já existe : Executando Save: ")
        if (modo_gravacao != "overwrite"){messageHeader.saveRaw(dfFinal, modo, partitions)}
        else{messageHeader.saveRaw(dfFinal, modo, partitions, modo_gravacao)}
    }
    println("")
}

// COMMAND ----------

// DBTITLE 1,Gera array com tabelas
//Cria arraybuffer vazio para armazenar tabelas distintas
var tables: ArrayBuffer[String] = ArrayBuffer.empty[String]

//gera DF contendo o nome de todas tabelas
val df_tabelas = df_body.select("destino.diretorio").distinct()
//Salva string contendo nome das tabelas no arraybuffer vazio
for (table <- df_tabelas.collect){
  println(s"Adicionando table ${table}")
  tables += table.getString(0)}

// COMMAND ----------

// DBTITLE 1,Extrai conteúdo / Salva na Tabela
//cria arrayBuffer vazio que ira armazenar tabelas que tiveram erro na sua execucao
var error_tables: ArrayBuffer[String] = ArrayBuffer.empty[String]
//cria value tables_widget que armazena as tabelas que devem ser processadas
//caso tables_widget esteja vazio todas as tabelas serao processadas
val tables_widget = dbutils.widgets.get("tables")

    if (tables_widget.length > 0){
        //transformando string de tables em Array
        val array_table : Array[String] = tables_widget.split(",")
        for (table_widget <- array_table){
            println(s"Executando processo individual para tabela : ${table_widget}")
            try{
                println(s"Carregando a tabela ${table_widget} para data_proc ${data_proc}")
                extractConteudo(df_body, table_widget)
                println("")}
            catch { case _: Exception =>
                println(s"Erro ao carregar tabela ${table_widget}")
                println("")
                error_tables += table_widget}
      }
    }
    else{println("tables_widget vazia : Executando processo para todas as tabelas")
        for (table <- tables){
            try{
                println(s"Carregando a tabela ${table} para data_proc ${data_proc}")
                extractConteudo(df_body, table)
                println("")}
            catch { case _: Exception => 
                println(s"Erro ao carregar tabela ${table}")
                println("")
                error_tables += table}
        }
     }



// COMMAND ----------

// DBTITLE 1,Verifica a execução
//condicao que retorna quais tabelas tiveram error na sua execucao
if (error_tables.length != 0){
    for (error <- error_tables){
        println(s"Falha na execucao da tabela : ${error}")}
    System.exit(2)}    
else{println("Todas as tabelas carregadas com SUCESSO")}
