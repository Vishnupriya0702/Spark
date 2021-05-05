# Spark


Sparkconf , sparkcontext are the ones which helps the application to run on your cluster.
Typically, spark runs each partition on each cluster. Typically, we want 2-3 partitions to run on each cluster. 
Spark tries to set the number of partitions automatically based on the current cluster. However, we can set  the parameter manually by passing it as second parameter sc.parallelize(data, 10)
