import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

val dateFormat = "yyyy-MM-dd"
val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
val dateString = "2020-04-27"
val d = java.time.LocalDate.parse(dateString, dtf).plusDays(-1)
val year_partition = d.getYear
val month_partition = d.getMonthValue
val day_partition = d.getDayOfMonth





val NewstocksSchema = StructType(Array(
  StructField("company", StringType),
  StructField("date", IntegerType),
  StructField("value", DoubleType)
))

val stocksSchema = StructType(Array(
    StructField("company", StringType),
    StructField("date", StringType),
    StructField("value", StringType)
  ))

var df = spark.read.format("csv").schema(stocksSchema).load("/home/flpp/Downloads/via_teste/struct/struct.csv")
df.printSchema()

for (row <- NewstocksSchema.fields){
    println(s"FIELD : ${row.name} | TYPE: ${row.dataType}")
    if (row.dataType.toString == "StringType"){
      println("ACTION : None")
    }
    else{
      println("ACTION : Cast")
      df = df.withColumn(row.name, col(row.name).cast(row.dataType))
    }
}

df.printSchema()
df.show()


val foo = 0
try {1 / foo } catch { case _: Exception => 2 / (foo + 1) } finally {println("bacon")}

val a : String = "bacon,frito,crocante"
val c : String = ""



val arrayA : Array[String] = a.split(",")
val arrayB : Array[String] = Array.empty[String]
val arrayC : Array[String] = c.split(",")


def thereIsBacon(array : Array[String]) : Unit = {
    if (array.length > 0)
        {println("Bacon")} 
    else {println("No Bacon")}
}

thereIsBacon(arrayA)









