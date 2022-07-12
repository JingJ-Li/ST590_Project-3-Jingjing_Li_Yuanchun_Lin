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
        SA_wrt=SA_df.iloc[i*500:(i+1)*500]
        SA_wrt.to_csv("prj3CSV/SA_wrt" + str(i) + ".csv",index=False,header = True)
    else:
        continue
    PC_wrt=PC_df.iloc[i*500:(i+1)*500]
    PC_wrt.to_csv("prj3CSV2/PC_wrt" + str(i) + ".csv",index=False,header = True)
    time.sleep(20)
    
## Reading a Stream    
from pyspark.sql.types import StructType
myschema=StructType().add("time","string").add("pid","string").add("x","float").add("y","float").add("z","float")
df_SA = spark.readStream.schema(myschema).csv("prj3CSV")
df_PC = spark.readStream.schema(myschema).csv("prj3CSV2") 

## Transform/Aggregation Step
from math import sqrt
from pyspark.sql.functions import *
df_SA2=df_SA.withColumn("mag_SA",sqrt(col("x")**2+col("y")**2+col("z")**2))
df_PC2=df_PC.withColumn("mag_PC",sqrt(col("x")**2+col("y")**2+col("z")**2))
df_SA3=df_SA2.select(["time","pid","mag_SA"])
df_PC3=df_PC2.select(["time","pid","mag_PC"])

## Writing the Streams
query_SA=df_SA3.writeStream\
               .format("csv")\
               .option("checkpointlocation","checkpoint_SA")\
               .option("path", "csv_SA")\
               .outputMode("append")\
               .start()
query_PC=df_PC3.writeStream\
               .format("csv")\
               .option("checkpointlocation","checkpoint_PC")\
               .option("path", "csv_PC")\
               .outputMode("append")\
               .start()


query_SA.stop()
query_PC.stop()

#Use PySpark to read in all "part" files
all_SA= spark.read.option("header","false").csv("csv_SA")
all_PC= spark.read.option("header","false").csv("csv_PC")

# Output as CSV file
all_SA \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("prj3CSV/single_csv_SA")
all_PC \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("prj3CSV2/single_csv_PC")