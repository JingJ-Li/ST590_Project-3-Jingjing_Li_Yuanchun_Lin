##############################
#ST590_Project3-YuanChun_Lin&Jingjing_Li
##############################

## First, we import all related packages
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
import os

# Set up for Creating Files
## We set the path folder and use pandas to read our csv data for all person id

path_folder='/Users/jingjingli/python/ST590_Project-3-Jingjing_Li_Yuanchun_Lin'
acc_df=pd.read_csv(path_folder+"/data/all_accelerometer_data_pids_13.csv")

##  Subset data by focusing on pid = 'SA0297' and 'PC6771'
df_1=acc_df[acc_df['pid']=='SA0297'] # 962,901 data in 'SA0297'
df_2=acc_df[acc_df['pid']=='PC6771'] # 2,141,701 data in 'PC6771'

## Define spark function 
spark=SparkSession.builder.appName('StructuredStreaming').getOrCreate()

## All what we've done is to set up for creating files
## Create a for loop to generate the split csv files which includes 500 rows data.
for i in range(df_2.shape[0]//500+1): # i from 0 to 4283
    if i <= df_1.shape[0]//500: # if i smaller than 1925
        if i!=df_1.shape[0]//500:
            temp=df_1.iloc[i*500:(i+1)*500,:]
            temp.to_csv(path_folder+'/data/SA0297/SA0297_'+str(i)+'.csv',index=False)
            time.sleep(20)
        else:
            temp=df_1.iloc[i*500:,:]
            temp.to_csv(path_folder+'/data/SA0297/SA0297_'+str(i)+'.csv',index=False)
            time.sleep(20)
    if i!=df_2.shape[0]//500:
        temp=df_2.iloc[i*500:(i+1)*500,:]
        temp.to_csv(path_folder+'/data/PC6771/PC6771_'+str(i)+'.csv',index=False)
        time.sleep(20)
    else:
        temp=df_2.iloc[i*500:,:]
        temp.to_csv(path_folder+'/data/PC6771/PC6771_'+str(i)+'.csv',index=False)
        time.sleep(20)

# Reading a Stream  
## Create an input stream and read it 
userschema=StructType().add("time","string").add("pid","string").add("x","float").add("y","float").add("z","float")
df_SA = spark.readStream.schema(userschema).csv(path_folder+"/data/SA0297")
df_PC = spark.readStream.schema(userschema).csv(path_folder+"/data/PC6771")


# Transform / Aggregation Step 
## We make aggregation about new column called mag_*
## mag_* column means the magnitude equals the squared root of total square sum of x, y, and z
##  Just keep the columns what we need
df_SA2=df_SA.withColumn("mag_SA",(col("x")**2+col("y")**2+col("z")**2)**0.5)
df_PC2=df_PC.withColumn("mag_PC",(col("x")**2+col("y")**2+col("z")**2)**0.5)

## Select desirable columns 
df_SA3=df_SA2.select(["time","pid","mag_SA"])
df_PC3=df_PC2.select(["time","pid","mag_PC"])

# Writing the Streams
## Now we try to write the streams: use format 'csv', outputMode 'append', two options 'checkpointlocation' and 'path'
## Reminder: Should remove all related folders first!
query_SA=df_SA3\
.writeStream\
.format("csv")\
.outputMode("append")\
.option("checkpointlocation",path_folder+"/checkpoint_SA")\
.option("path",path_folder+"/path_SA")\
.start()

query_PC=df_PC3\
.writeStream\
.format("csv")\
.outputMode("append")\
.option("checkpointlocation",path_folder+"/checkpoint_PC")\
.option("path",path_folder+"/path_PC")\
.start()

## We run writing-stream code about 5 mins
time.sleep(300)

## Then stop the query
query_SA.stop()
query_PC.stop()
    
# Output as CSV file
## Use PySpark to read in all output 'part' files in 'path' folder
allfiles_SA=spark.read.option("header","false").csv(path_folder+"/path_SA")
allfiles_PC=spark.read.option("header","false").csv(path_folder+"/path_PC")

## Output as a single CSV file
allfiles_SA.coalesce(1).write.format("csv").option("header","false").save(path_folder+"/data/final_SA")

allfiles_PC.coalesce(1).write.format("csv").option("header","false").save(path_folder+"/data/final_PC")

print('finish merge all csv!')