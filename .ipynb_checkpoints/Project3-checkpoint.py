## type pyspark in terminal
import pandas as pd

### Set up for Creating Files
acc_df=pd.read_csv("all_accelerometer_data_pids_13.csv")
df_1=acc_df[acc_df['pid']=='SA0297'] # 962,901 data in 'SA0297'
df_2=acc_df[acc_df['pid']=='PC6771'] # 2,141,701 data in 'PC6771'

## Split to several csv files
for i in range(df_2.shape[0]//500+1): # i from 0 to 4283
    if i <= df_1.shape[0]//500: # if i smaller than 1925
        if i!=df_1.shape[0]//500:
            temp=df_1.iloc[i*500:(i+1)*500,:]
            temp.to_csv(f'project3_data/SA0297/SA0297_'+str(i)+'.csv',index=False)
        else:
            temp=df_1.iloc[i*500:,:]
            temp.to_csv(f'project3_data/SA0297/SA0297_'+str(i)+'.csv',index=False)
    if i!=df_2.shape[0]//500:
        temp=df_2.iloc[i*500:(i+1)*500,:]
        temp.to_csv(f'project3_data/PC6771/PC6771_'+str(i)+'.csv',index=False)
    else:
        temp=df_2.iloc[i*500:,:]
        temp.to_csv(f'project3_data/PC6771/PC6771_'+str(i)+'.csv',index=False)

        
        
### Reading a Stream    
from pyspark.sql.types import StructType
userschema=StructType().add("time","string").add("pid","string").add("x","float").add("y","float").add("z","float")
df_SA = spark.readStream.option("sep",";").schema(userschema).csv("project3_data/SA0297/SA0297_*.csv")
df_PC = spark.readStream.option("sep",";").schema(userschema).csv("project3_data/PC6771/PC6771_*.csv")

### Transform / Aggregation Step\
from pyspark.sql.functions import *
df_SA2=df_SA.withColumn("mag_SA",(col("x")**2+col("y")**2+col("z")**2)**0.5)
df_PC2=df_PC.withColumn("mag_PC",(col("x")**2+col("y")**2+col("z")**2)**0.5)
df_SA3=df_SA2.select(["time","pid","mag_SA"])
df_PC3=df_PC2.select(["time","pid","mag_PC"])



## Writing the Streams
query_SA=df_SA3.writeStream.format("csv").trigger(processingTime="10 seconds").outputMode("append").option("checkpointlocation","checkpoint_SA").option("path","path_SA").start()

# query_SA=df_SA3.writeStream.format("csv").trigger(processingTime="20 seconds").outputMode("append").option("checkpointlocation","checkpoint_SA").option("path","path_SA").start()
query_SA.stop()

# query_PC=df_SA3.writeStream.format("csv").outputMode("append").option("checkpointlocation","checkpoint_PC")
# query_PC.start()
# query_PC.stop()

## Use PySpark to read in all "part" files
allfiles_SA= spark.read.option("header","false").csv("project3_data/SA0297/SA0297_*.csv")
allfiles_PC= spark.read.option("header","false").csv("project3_data/PC6771/PC6771_*.csv")

## Output as CSV file
allfiles_SA \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("/project3_data/final_SA.csv")

allfiles_PC \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("prj3CSV2/single_csv_PC/")