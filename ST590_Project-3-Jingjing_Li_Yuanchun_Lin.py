# Set up for Creating Files

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
for i in range (0, SA_df.shape[0]//500):
    SA_wrt=SA_df.loc[0:500]
    SA_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*499
    SA_wrt.to_csv("prj3CSV/SA_wrt" + str(i) + ".csv",index=False,header = True)
    PC_wrt=SA_df.loc[0:500]
    PC_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*499
    PC_wrt.to_csv("prj3CSV2/PC_wrt" + str(i) + ".csv",index=False,header = True)
    time.sleep(20)
    
## Reading a Stream    
from pyspark.sql.types import StructType
myschema=StructType().add("time","string").add("pid","string").add("x","integer").add("y","integer").add("z","integer").add("timestamp","timestamp")
df_SA = spark.readStream.schema(myschema).csv("prj3CSV")
df_PC = spark.readStream.schema(myschema).csv("prj3CSV2") 