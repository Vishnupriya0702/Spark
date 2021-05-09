# Spark


Sparkconf , sparkcontext are the ones which helps the application to run on your cluster.
Typically, spark runs each partition on each cluster. Typically, we want 2-3 partitions to run on each cluster. 
Spark tries to set the number of partitions automatically based on the current cluster. However, we can set  the parameter manually by passing it as second parameter sc.parallelize(data, 10)


Difference between RDD & DataFrames:
1. to open the file : RDD - sc.textFile, sc.WholetextFiles
                      DataFrame - spark.read.csv
                      
      
      
DATAFRAMES SESSION Code and API's :  
  Dataframe Functions : Select, PrintSchema, show, describe
  difference between PrintSchema & Describe :
  PrintSchema - prints the schema
  >>> ordersDF.printSchema()
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)

  Describe - show detailed description of each column:
  >>> ordersDF.describe()
+-------+------------------+--------------------+-----------------+---------------+
|summary|               _c0|                 _c1|              _c2|            _c3|
+-------+------------------+--------------------+-----------------+---------------+
|  count|             68883|               68883|            68883|          68883|
|   mean|           34442.0|                null|6216.571098819738|           null|
| stddev|19884.953633337947|                null|3586.205241263963|           null|
|    min|                 1|2013-07-25 00:00:...|                1|       CANCELED|
|    max|              9999|2014-07-24 00:00:...|             9999|SUSPECTED_FRAUD|
+-------+------------------+--------------------+-----------------+---------------+

Conversion of Dataframe to RDD:
>>> ordersDF.rdd
MapPartitionsRDD[35] at javaToPython at NativeMethodAccessorImpl.java:0
>>> c= ordersDF.rdd
>>> c.take(10)
>>> 
rdd doesnt contain show API. Only Dataframe has Show API.

How to connect to the Spark Sql from Dataframe:
Using the API - CreateTempView
>>> ordersDF.createTempView('orders')
>>> spark.sql('select * from orders')

Note: Important point: In the commandline prompt, there is no need to import the sparksession. But, when it comes to Pycharm & other IDE's, we have to import the pycharm session
using Sparksession object called spark.

API to read fixed length rows:
ordersDF =spark.read.text("/public/retail_db/orders")
>>> ordersDF.show()
+--------------------+
|               value|
+--------------------+
|1,2013-07-25 00:0...|
|2,2013-07-25 00:0...|
|3,2013-07-25 00:0...|
|4,2013-07-25 00:0...|
|5,2013-07-25 00:0...|
|6,2013-07-25 00:0...|
|7,2013-07-25 00:0...|
|8,2013-07-25 00:0...|
|9,2013-07-25 00:0...|
|10,2013-07-25 00:...|
|11,2013-07-25 00:...|
|12,2013-07-25 00:...|
|13,2013-07-25 00:...|
|14,2013-07-25 00:...|
|15,2013-07-25 00:...|
|16,2013-07-25 00:...|
|17,2013-07-25 00:...|
|18,2013-07-25 00:...|
|19,2013-07-25 00:...|
|20,2013-07-25 00:...|
+--------------------+

to show all the data, we have to type the API as follows:
 ordersDF.show(truncate=False)
+---------------------------------------------+
|value                                        |
+---------------------------------------------+
|1,2013-07-25 00:00:00.0,11599,CLOSED         |
|2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT  |
|3,2013-07-25 00:00:00.0,12111,COMPLETE       |
|4,2013-07-25 00:00:00.0,8827,CLOSED          |
|5,2013-07-25 00:00:00.0,11318,COMPLETE       |
|6,2013-07-25 00:00:00.0,7130,COMPLETE        |
|7,2013-07-25 00:00:00.0,4530,COMPLETE        |
|8,2013-07-25 00:00:00.0,2911,PROCESSING      |
|9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT |
|10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT|
|11,2013-07-25 00:00:00.0,918,PAYMENT_REVIEW  |
|12,2013-07-25 00:00:00.0,1837,CLOSED         |
|13,2013-07-25 00:00:00.0,9149,PENDING_PAYMENT|
|14,2013-07-25 00:00:00.0,9842,PROCESSING     |
|15,2013-07-25 00:00:00.0,2568,COMPLETE       |
|16,2013-07-25 00:00:00.0,7276,PENDING_PAYMENT|
|17,2013-07-25 00:00:00.0,2667,COMPLETE       |
|18,2013-07-25 00:00:00.0,1205,CLOSED         |
|19,2013-07-25 00:00:00.0,9488,PENDING_PAYMENT|
|20,2013-07-25 00:00:00.0,9198,PROCESSING     |
+---------------------------------------------+

Creating Dataframe:
df1=sqlContext.createDataFrame(ordersrdd.map( lambda x: Row(x.split(",")[0], x.split(",")[1], x.split(",")[2], x.split(",")[3])),
StructType([StructField("OrderID",StringType(), True),StructField("Order_date",StringType(), True),
StructField("Product_id",StringType(), True),StructField("Order_status",StringType(), True)]))

Dataframe with Hive:

