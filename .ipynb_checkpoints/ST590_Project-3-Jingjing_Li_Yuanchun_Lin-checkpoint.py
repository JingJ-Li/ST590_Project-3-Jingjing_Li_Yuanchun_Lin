# Set up for Creating Files

## Set up for Creating Files 

### Import modules
import pandas as pd

### Read in data
acc_df = pd.read_csv ("data/all_accelerometer_data_pids_13.csv")

# Subset data 
SA_df= acc_df[acc_df.pid=="SA0297"]
PC_df= acc_df[acc_df.pid=="PC6771"]

# Write 500 values to the data
for i in range (0, SA_df.shape[0]//500):
    SA_wrt=SA_df.loc[0:500]
    SA_wrt["timestamp"]=[time.strftime("%H:%M:%S",time.localtime())]*500
    SA_wrt.to_csv("prj3CSV/SA_wrt" + str(i) + ".csv",index=False)
    time.sleep(20)
     