// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalake2231.dfs.core.windows.net",
    dbutils.secrets.get(scope="datalake2231_key", key="datalake2231-secret")
    )

val file_loc = "abfss://csv@datalake2231.dfs.core.windows.net/Log.csv"
val file_type = "csv"

// COMMAND ----------

val schema = StructType(Array(
  StructField("Coorelationid", StringType, true),
  StructField("Operationname", StringType, true),
  StructField("Status", StringType, true),
  StructField("Eventcategory", StringType, true),
  StructField("Level", StringType, true),
  StructField("Time", StringType, true),
  StructField("Subscription", StringType, true),
  StructField("Eventinitiatedby", StringType, true),
  StructField("Resourcetype", StringType, true),
  StructField("Resourcegroup", StringType, true),
  StructField("Resource", StringType, true)
))

var df = spark.read.format(file_type)
.options(Map("header" -> "true"))
.schema(schema)
.load(file_loc)

display(df)



// COMMAND ----------


