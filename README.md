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

Accessing Hive tables using Dataframe:
Using read.table API - we can connect to the database.tablename
 orders =spark.read.table('itv000076_retaildb_txt.orders')
>>> orders.show()
+--------+----------+-----------------+---------------+
|order_id|order_date|order_customer_id|   order_status|
+--------+----------+-----------------+---------------+
|       1|      null|            11599|         CLOSED|
|       2|      null|              256|PENDING_PAYMENT|
|       3|      null|            12111|       COMPLETE|
|       4|      null|             8827|         CLOSED|
|       5|      null|            11318|       COMPLETE|
|       6|      null|             7130|       COMPLETE|
|       7|      null|             4530|       COMPLETE|
|       8|      null|             2911|     PROCESSING|
|       9|      null|             5657|PENDING_PAYMENT|
|      10|      null|             5648|PENDING_PAYMENT|
|      11|      null|              918| PAYMENT_REVIEW|
|      12|      null|             1837|         CLOSED|
|      13|      null|             9149|PENDING_PAYMENT|
|      14|      null|             9842|     PROCESSING|
|      15|      null|             2568|       COMPLETE|
|      16|      null|             7276|PENDING_PAYMENT|
|      17|      null|             2667|       COMPLETE|
|      18|      null|             1205|         CLOSED|
|      19|      null|             9488|PENDING_PAYMENT|
|      20|      null|             9198|     PROCESSING|
+--------+----------+-----------------+---------------+

Connecting to Other Databases requires jdbc file and we need to specify the Database name, tablename & other properties such as username, password and load options.
the syntax would be :
3 main approaches:
1.spark.read.format('jdbc').option('url','jdbc:mysql://ms.itversity.com:3306').option('dbtable','retail_db_order_items').option('user','retail_user').option('password','itversity').load()
2. spark.read.jdbc("mysql://ms.itversity.com:3306").dbtable('retail_db_order_items').username & password as dictionary.
3. Sqoop

Partitions also can be passed as one of the option such as numpartitions, Lowerbound & upperbound.
Partitions describe the number of partitions. with lowerbound and upperbound values. Also, we have to decide on the partitions too. Lowerbound & Upperbound are more useful in determining the total records in each partitions.

this option is similar to Sqoop.
Similar to abinitio - how mfs partitins are created - numpartitions, lowerbound & upperbound is used to create the total partitions.

Difference between CreateDataframe and read.csv:
CreateDataframe & toDF - converts RDD to Dataframe.
read-csv - mainly used to read a data from a file and then create a Dataframe.

Applying schema as below :
 ordersDF1 =spark.read.csv("/public/retail_db/order_items", schema='order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, order_item_subtotal float,order_item_product_price float')

Usage of withColumn: to cast the column type as below :
OrdersDF.withColumn('columnname' 'dataframe.columnname.cast())
         withColumn('columnname' 'dataframe.columnname.filter())


Select & projection of data in Dataframe:
DF.select('column names')
DF.withColumn('column to be added', transformation function)
Alias: Alias function is mainly used as like below :
        DF.select(column names, transfomation function.alias( alias column name))
 
 Using Hive form of Select:
 DF.SelectExpr(substring(order_date, 1,7) as column_name)
 drop is also used.
 
 Synatx for Filter & Join:
 FIlter : Where( columnname.isin( ))
 Join : >>> ordersjoin = ordersFiltered.join(ordersDF1, ordersFiltered.order_id == ordersDF1.order_item_order_id, 'leftouter')
>>> ordersjoin.count()
80597
>>> ordersjoin1 = ordersFiltered.join(ordersDF1, ordersFiltered.order_id == ordersDF1.order_item_order_id, 'inner')
>>> ordersjoin1.count()
75408

Aggregations :
Few functions such as distinct (count), Sum can be applied directly to the Dataframe as below :

Distinct function & CountDistinct function:
>>> from pyspark.sql.functions import countDistinct
>>> ordersDF.select(countDistinct('order_status')).show()
+----------------------------+
|count(DISTINCT order_status)|
+----------------------------+
|                           9|
+----------------------------+

>>> ordersDF.select('order_status').count().distinct().show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'int' object has no attribute 'distinct'
>>> ordersDF.select('order_status').distinct().show()
+---------------+
|   order_status|
+---------------+
|PENDING_PAYMENT|
|       COMPLETE|
|        ON_HOLD|
| PAYMENT_REVIEW|
|     PROCESSING|
|         CLOSED|
|SUSPECTED_FRAUD|
|        PENDING|
|       CANCELED|
+---------------+

>>> ordersDF.select('order_status').distinct().count().show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'int' object has no attribute 'show'
>>> ordersDF.select('order_status').distinct().count()
9

sum:
total aggregation - use sum on total:
>>> from pyspark.sql.functions import sum, round
>>> ordersDF1.select(sum('order_item_subtotal')).show()
+------------------------+
|sum(order_item_subtotal)|
+------------------------+
|     3.432262059842491E7|
+------------------------+

To group based on column names, we can give the function like below :

ordersDF1.select('order_item_order_id',ordersDF1.groupBy('order_item_order_id').agg(sum('order_item_subtotal').alias('order_revenue')))
agg helps to provide alias name and propagate along with it.

Sort :
ordersDF.sort(['order_id','order_date'],ascending=[1,0]).show()
ordersDF.sort('ordersDF.order_id.desc()','order_date').show()
Sort within Groups -ordersDF.sortWithinPartitions(['order_date','order_status'],ascending=[1,0]).show()

Spark with MachineLearning :
  Linear Regression : This part comes under classification section.Linear Regression Model is designed to perform Predictive analysis.
  RegParam - Regularization Parameter. It should be greater than 0. Default is 0.0
  ElasticNetParam : This indicates alpha(0,1). This also predicts the output based on the parameter inputs.
  
  Pipeline : Build a pipeline based on the input values.
  ParamGridBuilder : Is to add the parameters needed for the Predictive analysis.
  
  Crossvalidator - splits the data into training & test data and performs the validation.
  Evaluator : Evalutor performs the data regression at maximum level. Evalutor can be RegressionEvalutor, CrossvalidationEvalutor.
  Fit - it helps to fit the exact model to the Evalutor.
  Transform - transform from tarining to test.
  Correlation - Correlaation between 2 variables. 
  


 
