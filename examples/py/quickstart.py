# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("Milvus Integration") \
#     .master("local[*]") \
#     .getOrCreate()

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# .option("milvus.host", "localhost") \
# .option("milvus.port", "19530") \

columns = ["id", "text", "vec"]
data = [(1, "a", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (2, "b", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (3, "c", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (4, "d", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0])]
sample_df = spark.sparkContext.parallelize(data).toDF(columns)
sample_df.write \
    .mode("append") \
    .option("milvus.uri", "https://in01-1479b85614880b2.aws-us-west-2.vectordb-uat3.zillizcloud.com:19537") \
    .option("milvus.token", "root:n7}y3XJb4<oxMORqRU!!TMn1swDK4[]Q") \
    .option("milvus.collection.name", "hello_spark_milvus_fubang") \
    .option("milvus.collection.vectorField", "vec") \
    .option("milvus.collection.vectorDim", "8") \
    .option("milvus.collection.primaryKeyField", "id") \
    .format("milvus") \
    .save()