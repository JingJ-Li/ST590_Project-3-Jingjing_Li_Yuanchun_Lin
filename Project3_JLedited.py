##############################
ST590_Project3-YuanChun_Lin&Jingjing_Li
##############################

# Set up for Creating Files
## First type pyspark in terminal
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
import os

## Read in data
acc_df=pd.read_csv("data/all_accelerometer_data_pids_13.csv")

## Subset data 
df_1=acc_df[acc_df['pid']=='SA0297'] # 962,901 data in 'SA0297'
df_2=acc_df[acc_df['pid']=='PC6771'] # 2,141,701 data in 'PC6771'

## Define spark function 
spark=SparkSession.builder.appName('StructuredStreaming').getOrCreate()

start_time=time.time()

##  Write 500 values at a time to a csv file
for i in range(df_2.shape[0]//500+1): # i from 0 to 4283
    if i <= df_1.shape[0]//500: # if i smaller than 1925
        if i!=df_1.shape[0]//500:
            temp=df_1.iloc[i*500:(i+1)*500,:]
            temp.to_csv('data/SA0297/SA0297_'+str(i)+'.csv',index=False)
        else:
            temp=df_1.iloc[i*500:,:]
            temp.to_csv('data/SA0297/SA0297_'+str(i)+'.csv',index=False)
    if i!=df_2.shape[0]//500:
        temp=df_2.iloc[i*500:(i+1)*500,:]
        temp.to_csv('data/PC6771/PC6771_'+str(i)+'.csv',index=False)
    else:
        temp=df_2.iloc[i*500:,:]
        temp.to_csv('data/PC6771/PC6771_'+str(i)+'.csv',index=False)
    time.sleep(20)
    
#Reading a Stream    
userschema=StructType().add("time","string").add("pid","string").add("x","float").add("y","float").add("z","float")
df_SA = spark.readStream.schema(userschema).csv("data/SA0297")
df_PC = spark.readStream.schema(userschema).csv("data/PC6771")

# Transform / Aggregation Step\
### Add new columns with mega value
df_SA2=df_SA.withColumn("mag_SA",(col("x")**2+col("y")**2+col("z")**2)**0.5)
df_PC2=df_PC.withColumn("mag_PC",(col("x")**2+col("y")**2+col("z")**2)**0.5)

## Select desirable columns 
df_SA3=df_SA2.select(["time","pid","mag_SA"])
df_PC3=df_PC2.select(["time","pid","mag_PC"])

# Writing the Streams
## Should remove all folders first!
## Writing the stream of SA0297 data
query_SA=df_SA3\
.writeStream\
.format("csv")\
.outputMode("append")\
.option("checkpointlocation","checkpoint_SA")\
.option("path","path_SA")\
.start()
  
## Writing the stream of PC6771 data
query_PC=df_PC3\
.writeStream\
.format("csv")\
.outputMode("append")\
.option("checkpointlocation","checkpoint_PC")\
.option("path","path_PC")\
.start()

### Stop stream data query
query_SA.stop()
query_PC.stop()

print('Finish part '+str(i)+'!')

end_time=time.time()

if end_time-start_time>330:
    print('Break out of loop!')
    break

## Use PySpark to read in all "part" files
allfiles_SA=spark.read.option("header","false").csv("path_SA")
allfiles_PC=spark.read.option("header","false").csv("path_PC")

## Output as CSV file
allfiles_SA.coalesce(1).write.format("csv").option("header","false").save("project3_data/final_SA")
allfiles_PC.coalesce(1).write.format("csv").option("header","false").save("project3_data/final_PC")

print('finish merge all csv!')