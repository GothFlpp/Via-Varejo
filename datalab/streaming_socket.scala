import org.apache.spark.sql.functions.{col, lit, when, length}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.streaming.{Trigger}



def readStreamSocket() = {
    var lines = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 1234)
        .load()

    lines = lines.withColumn("len", length($"value"))

    println(lines.isStreaming)

    val query = lines.writeStream
        .format("console")
        .outputMode("append")
        .start()
    
}




def readStreamFile() = {

    val schema = StructType(Array(
        StructField("id", StringType),
        StructField("name", StringType),
        StructField("surname", StringType),
        StructField("gender", StringType)))

    var lines = spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load("/home/flpp/source")

    println(lines.isStreaming)

    val query = lines.writeStream
        .format("console")
        .outputMode("append")
        .start()
    
}

def demotrigger() = {

    val schema = StructType(Array(
        StructField("id", StringType),
        StructField("name", StringType),
        StructField("surname", StringType),
        StructField("gender", StringType)))

    var lines = spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load("/home/flpp/source")

    println(lines.isStreaming)

    val query = lines.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("3 seconds"))
        .outputMode("append")
        .start()

    query.awaitTermination()

}


demotrigger()
readStreamSocket()
readStreamFile()