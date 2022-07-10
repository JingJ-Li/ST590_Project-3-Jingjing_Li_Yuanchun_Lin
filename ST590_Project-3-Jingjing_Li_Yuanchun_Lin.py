##############################
ST590_Project3-Jingjing_Li&YuanChun_Lin
##############################

## Set up for Creating Files 

### Import modules
import pandas as pd
import time

### Read in data
acc_df = pd.read_csv ("data/all_accelerometer_data_pids_13.csv")

### Subset data 
SA_df= acc_df[acc_df.pid=="SA0297"]
PC_df= acc_df[acc_df.pid=="PC6771"]

### Write 500 values to the data
#for i in range (0, SA_df.shape[0]//500):
for i in range (0,len(PC_df)//500):
    if i < len(SA_df)//500:
        SA_wrt=SA_df.loc[0:500]
        SA_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*499
        SA_wrt.to_csv("prj3CSV/SA_wrt" + str(i) + ".csv",index=False,header = True)
        
    else:
        continue
    i +=1
    PC_wrt=PC_df.loc[0:500]
    PC_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*499
    PC_wrt.to_csv("prj3CSV2/PC_wrt" + str(i) + ".csv",index=False,header = True)
    time.sleep(20)
    
## Reading a Stream    
from pyspark.sql.types import StructType
myschema=StructType().add("time","string").add("pid","string").add("x","integer").add("y","integer").add("z","integer").add("timestamp","timestamp")
df_SA = spark.readStream.schema(myschema).csv("prj3CSV/SA_wrt*.csv")
df_PC = spark.readStream.schema(myschema).csv("prj3CSV2/PC_wrt*.csv") 

## Transform/Aggregation Step
from math import sqrt
from pyspark.sql.functions import *
df_SA2=df_SA.withColumn("mag_SA",sqrt(col("x")**2+col("y")**2+col("z")**2))
df_PC2=df_PC.withColumn("mag_SA",sqrt(col("x")**2+col("y")**2+col("z")**2))
df_SA3=df_SA2.select(["time","pid","mag_SA"])
df_PC3=df_PC2.select(["time","pid","mag_SA"])

## Writing the Streams
query_SA=df_SA3.writeStream\
               .format("csv")\
               .option("checkpointlocation","checkpoint_SA")\
               .option("path", "csv_SA")\
               .outputMode("append")
query_SA.start()
query_PC=df_PC3.writeStream\
               .format("csv")\
               .option("checkpointlocation","checkpoint_PC")\
               .option("path", "csv_PC")\
               .outputMode("append")
query_PC.start()


query_SA.stop()
query_PC.stop()

#Use PySpark to read in all "part" files
all_SA= spark.read.option("header","false").csv("prj3CSV/SA_wrt*.csv")
all_PC= spark.read.option("header","false").csv("prj3CSV2/PC_wrt*.csv")

# Output as CSV file
all_SA \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("prj3CSV/single_csv_SA/")
all_PC \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("prj3CSV2/single_csv_PC/")