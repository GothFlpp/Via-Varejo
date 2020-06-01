import org.apache.spark.sql.functions.{col, lit, when, length, from_json}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.streaming.{Trigger}

spark.conf.set("spark.sql.shuffle.partitions",5)

def joinning() = {

    val schema = StructType(Array(
        StructField("id", StringType),
        StructField("name", StringType)))

    val schema2 = StructType(Array(
        StructField("id", StringType),
        StructField("surname", StringType)))

    var lines = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 1234)
        .load()
        .select(from_json(col("value"), schema).as("names"))
        .select("names.id", "names.name")

    var lines2 = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 1235)
        .load()
        .select(from_json(col("value"), schema2).as("surnames"))
        .select("surnames.id", "surnames.surname")

    val joinned = lines.join(lines2, lines.col("id") === lines2.col("id"))
        .select(lines.col("id"), lines.col("name"), lines2.col("surname"))

    val query = joinned.writeStream
        .format("console")
        .outputMode("append")
        .start()

}

joinning()


{"id":1,"name":"Felippe"}
{"id":2,"name":"Irina"}
{"id":3,"name":"Dedy"}


{"id":1,"surname":"Bacchi"}
{"id":2,"surname":"Martinelli"}
{"id":3,"surname":"Bacchi"}