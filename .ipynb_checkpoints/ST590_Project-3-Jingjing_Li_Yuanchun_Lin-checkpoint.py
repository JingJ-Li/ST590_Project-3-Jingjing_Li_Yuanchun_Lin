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
for i in range (0,10):
    SA_wrt=SA_df.loc[0:500]
    SA_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*499
    SA_wrt.to_csv("prj3CSV/SA_wrt" + str(i) + ".csv",index=False,header = True)
    mag_SA=math.sqrt(df_SA.x**2+df_SA.y**2+df_SA.z**2)_wrt=SA_df.loc[0:500]
    PC_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*499
    PC_wrt.to_csv("prj3CSV2/PC_wrt" + str(i) + ".csv",index=False,header = True)
    time.sleep(20)
    
## Reading a Stream    
from pyspark.sql.types import StructType
myschema=StructType().add("time","string").add("pid","string").add("x","integer").add("y","integer").add("z","integer").add("timestamp","timestamp")
df_SA = spark.readStream.schema(myschema).csv("prj3CSV")
df_PC = spark.readStream.schema(myschema).csv("prj3CSV2") 

## Transform/Aggregation Step
from math import sqrt
from pyspark.sql.functions import *
df_SA2=df_SA.withColumn("mag_SA",sqrt(col("x")**2+col("y")**2+col("z")**2))
df_PC2=df_PC.withColumn("mag_SA",sqrt(col("x")**2+col("y")**2+col("z")**2))
df_SA3=df_SA2.select(["time","pid","mag_SA"])
df_PC3=df_PC2.select(["time","pid","mag_SA"])

## Writing the Streams
query_SA=df_SA3.writeStream.format("csv").outputMode("append").option("checkpointlocation","prj3CSV")
query_PC=df_SA3.writeStream.format("csv").outputMode("append").option("checkpointlocation","prj3CSV2")
query_SA.start()
query_PC.start()