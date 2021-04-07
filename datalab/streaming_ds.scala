import org.apache.spark.sql.functions.{col, lit, when, length, from_json}
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
import org.apache.spark.sql.streaming.{Trigger}
import org.apache.spark.sql.Dataset
import spark.implicits._

case class dados (
    id : Long,
    name : String
)

def dfToDS() : Dataset[dados] = {

    val schema = StructType(Array(
        StructField("id", LongType),
        StructField("name", StringType)))

    spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 1234)
        .load()
        .select(from_json(col("value"), schema).as("names"))
        .select("names.id", "names.name")
        .as[dados]
}

def calling() = {
    val dsDados : Dataset[dados] = dfToDS()

    val dsSelect = dsDados.map(_.name)

    dsSelect.writeStream
        .format("console")
        .outputMode("append")
        .start()
}


{"id":1,"name":"Felippe"}
{"id":2,"name":"Irina"}
{"id":3,"name":"Dedy"}