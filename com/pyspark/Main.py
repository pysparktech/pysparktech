from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv reading").master("local").getOrCreate()

filepath = "C:\\Users\\viewp\\OneDrive\\Desktop\\data\\accidents_2017.csv"

#create table tablename(column datatype.......)
# spark : _c0........... (string)
# storage :" file formats -> parquet , orc, rc, csv, text, json, xml, avro .... etc.,
readOptions = {
    "header":"true",
    "inferSchema":"true"
}
#df = spark.read.option("header","true").option("inferSchema","true").csv(filepath)
df = spark.read.csv(filepath,**readOptions)

#desc tablename
df.printSchema()

# select * from table limit 5
df.show(5,truncate=0)

targetPath = "C:\\Users\\viewp\\OneDrive\\Desktop\\data\\output"
# insert into targetable (.......)
df.write.mode("overwrite").parquet(targetPath)
