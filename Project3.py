## First type pyspark in terminal
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
import os

### Set up for Creating Files
path_folder='/Users/YuanChunLin/Desktop/Python/ST590/Project 3'
acc_df=pd.read_csv(path_folder+"/all_accelerometer_data_pids_13.csv")
df_1=acc_df[acc_df['pid']=='SA0297'] # 962,901 data in 'SA0297'
df_2=acc_df[acc_df['pid']=='PC6771'] # 2,141,701 data in 'PC6771'

spark=SparkSession.builder.appName('StructuredStreaming').getOrCreate()

start_time=time.time()


### TEST!
for i in range(df_2.shape[0]//500+1): # i from 0 to 4283
    if i <= df_1.shape[0]//500: # if i smaller than 1925
        if i!=df_1.shape[0]//500:
            temp=df_1.iloc[i*500:(i+1)*500,:]
            temp.to_csv(path_folder+'/data/SA0297/SA0297_'+str(i)+'.csv',index=False)
        else:
            temp=df_1.iloc[i*500:,:]
            temp.to_csv(path_folder+'/data/SA0297/SA0297_'+str(i)+'.csv',index=False)
    if i!=df_2.shape[0]//500:
        temp=df_2.iloc[i*500:(i+1)*500,:]
        temp.to_csv(path_folder+'/data/PC6771/PC6771_'+str(i)+'.csv',index=False)
    else:
        temp=df_2.iloc[i*500:,:]
        temp.to_csv(path_folder+'/data/PC6771/PC6771_'+str(i)+'.csv',index=False)
    
    ### Reading a Stream    
    userschema=StructType().add("time","string").add("pid","string").add("x","float").add("y","float").add("z","float")
    # df_SA = spark.readStream.schema(userschema).csv(path_folder+"/data/SA0297")
    # df_PC = spark.readStream.schema(userschema).csv(path_folder+"/data/PC6771")
    df_SA = spark.readStream.schema(userschema).csv(path_folder+"/project3_data/SA0297")
    df_PC = spark.readStream.schema(userschema).csv(path_folder+"/project3_data/PC6771")

    ### Transform / Aggregation Step\
    df_SA2=df_SA.withColumn("mag_SA",(col("x")**2+col("y")**2+col("z")**2)**0.5)
    df_PC2=df_PC.withColumn("mag_PC",(col("x")**2+col("y")**2+col("z")**2)**0.5)
    df_SA3=df_SA2.select(["time","pid","mag_SA"])
    df_PC3=df_PC2.select(["time","pid","mag_PC"])

    ## Writing the Streams
    ## Should remove all folders first!
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

    time.sleep(20)
    
    query_SA.stop()
    query_PC.stop()
    
    print('Finish part '+str(i)+'!')
    
    end_time=time.time()
    
    if end_time-start_time>330:
        print('Break out of loop!')
        break


## Use PySpark to read in all "part" files
allfiles_SA=spark.read.option("header","false").csv(path_folder+"/path_SA")
allfiles_PC=spark.read.option("header","false").csv(path_folder+"/path_PC")

## Output as CSV file
allfiles_SA.coalesce(1).write.format("csv").option("header","false").save(path_folder+"/project3_data/final_SA")

allfiles_PC.coalesce(1).write.format("csv").option("header","false").save(path_folder+"/project3_data/final_PC")

print('finish merge all csv!')