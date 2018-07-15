
# coding: utf-8

# In[ ]:

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.types import *
import bus_times


# In[ ]:

def toCSV(x):
    y=str(x[0])
    for i in range(1,len(x)):
       y= y+","+str(x[i]) 
    return y

    
##########################################################################
#conf = SparkConf().setMaster("spark://King:7077").setAppName("batch_Main")
conf = SparkConf().setAppName("batch_Main")


sc = SparkContext(conf = conf)
sc.addPyFile("/home/bdata/sparkPrograms/BD-Traffic/bus_times.py")

#rdd = sc.textFile('file:///home/cloudera/Downloads/MTA-Bus-Time_.2014-08-01.txt')

rdd = sc.textFile('hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-10-31.txt')
#rdd = sc.textFile('hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08-*')
#rdd = sc.textFile('hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08-[01-15]*')
#rdd = sc.textFile("hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08*,hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-09-[01-15]*")
#rdd = sc.textFile('hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-0*,hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-10-[01-15]*')

classTuple= bus_times.mainFilter(rdd)
halfHourBucket=classTuple.map(lambda x: bus_times.toHalfHourBucket(list(x)))

halfHourBucket=halfHourBucket.map(lambda x: [int(x[0]),str(x[1]),str(x[2]),int(x[3]),float(x[4]),float(x[5])]) 
##########################################################################   
#CSV
#classTuple.map(lambda x: toCSV(x)).saveAsTextFile('hdfs:/user/bdata/demo_test.csv')

#halfHourBucket.map(lambda x: toCSV(x)).saveAsTextFile('hdfs://King:9000/user/bdata/demo_test_halloween.csv')


###########################################################################
##RDD TO DATAFRAME
sparkMach_session = SparkSession.builder \
.master("spark://King:7077") \
.appName("sparkmach") \
.config("spark.driver.allowMultipleContexts", "true") \
.getOrCreate()

bucket_schema= StructType([StructField("bus_id",IntegerType(), True),StructField("route_id",StringType(), True),StructField("next_stop_id",StringType(), True),StructField("direction",IntegerType(), True),StructField("half_hour_bucket",FloatType(), True),StructField("class",FloatType(), True) ])

times_df= sparkMach_session.createDataFrame(halfHourBucket, bucket_schema)

times_df.show()
###########################################################################
#RDD
#Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
#classTuple= formatTime.map(lambda x: (x[2],x[11],x[3],x[5],x[10],x[7]))

#classTuple.saveAsTextFile('hdfs:/user/bdata/classification_buses_test.txt')


#Para correr pruebas quitar comentario
#for i in halfHourBucket.take(20):

#for i in getResultsFromOneBus.take(3):
#for i in getResultsFromOneRoute.take(100):
     
	# print(i)


# In[ ]:

#print(classTuple.count())


# In[ ]:




# In[ ]:



