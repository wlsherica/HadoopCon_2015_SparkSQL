#SparkSQL training materials
#data size: 2.1MB
#network traffic data (TCP)
#homepage: http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html

#coding book
#   duration: length (number of seconds) of the connection 
#   protocol_type: type of the protocol, e.g. tcp, udp, etc. 
#   service: network service on the destination, e.g., http, telnet, etc. 
#   flag: normal or error status of the connection 
#   src_bytes: number of data bytes from source to destination 
#   dst_bytes: number of data bytes from destination to source 

#start 
#data preparation
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "/home/erica/kddcup.data_10_percent.gz")

#save data as local
data_file = "file:///home/erica/kddcup.data_10_percent.gz"
#cache data_file in cache() for the following use
raw_data = sc.textFile(data_file).cache()

#import library we need
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

#split raw_data
csv_data = raw_data.map(lambda l: l.split(","))
#register its schema
row_data = csv_data.map(lambda p: Row(
    duration = int(p[0]), 
    protocol_type = p[1],
    service = p[2],
    flag = p[3],
    src_bytes = int(p[4]),
    dst_bytes = int(p[5]),
    label = p[41]
    )
)

#list what we have
sqlContext.sql("show tables").show()

#transform pythonRDD to dataframe
interactions_df = sqlContext.createDataFrame(row_data)
#register temp table
interactions_df.registerTempTable("interactions")

#check our table list again
sqlContext.sql("show tables").show()

#print schema format
interactions_df.printSchema()
#query top 2 row data
interactions_df.show(2)


#with sql as usual
sqlContext.sql("select     service,\ 
                           count(*) as cnt\
                from       interactions \
                group by   service").show()

sqlContext.sql("select     service,\
                           count(*) as cnt\ 
                from       interactions\ 
                group by   service\
                order by   cnt ").show()

#with DataFrame
interactions_df.groupBy('service').count().show()

interactions_df.select("flag", "protocol_type")\
               .groupBy("flag")\
               .count()\
               .show()

#filter (where statement)
interactions_df.filter(interactions_df.duration > 1000).show()

interactions_df.describe('duration').show()
interactions_df.stat.corr("duration","dst_bytes")

interactions_df.stat.crosstab("protocol_type","flag").show()


sqlContext.sql("select     protocol_type,\
                           count(*) as cnt,\
                           sum(duration) as total_duration\
                from       interactions\
                group by   protocol_type").show()


sqlContext.sql("select     protocol_type,\
                           count(*) as cnt,\
                           avg(duration) as total_duration\
                  from     interactions\
                group by   protocol_type").show()

#udf in DF
label_fun = udf(lambda x: "normal" if x == "normal." else "attack", StringType())

#udf in sqlContext
sqlContext.registerFunction('strLength', lambda x: len(x))

#add attack column
df2 = interactions_labeled_df.withColumn('attack',label_fun(interactions_labeled_df['label']))
df2.groupBy("attack").count().show()

interactions_labeled_df.registerTempTable("interactions_label")

sqlContext.sql("select     label,\
                           case when label = 'normal.'\
                                then 'normal'\
                                else 'attack'\
                                end as attack\
                  from     interactions_label")\
              .groupBy('attack')\
              .count()\
              .show()

df3 = sqlContext.sql("select   *,\
                               case when label = 'normal.'\
                                    then 'normal'\
                                    else 'attack'\
                                    end as attack\
                        from   interactions_label")

df3.show(3)

