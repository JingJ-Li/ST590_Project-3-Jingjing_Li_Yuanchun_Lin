# Task 1 

## Reading a Stream
df=spark.readStream.format('rate').option('rowsPerSecond',1).load()
writeRate=df.writeStream.outputMode('append').format('console').start()
writeRate.stop()

## Transform/Aggregation Step
from pyspark.sql.functions import window
agg = df.withWatermark("timestamp", "5 seconds").groupBy(window(df.timestamp, "30 seconds", "30 seconds")).sum()

## Writing the Stream
myquery=agg.writeStream.outputMode('update').format('memory').trigger(processingTime='2 seconds').queryName('name_goes_here').start()
myquery.stop()

## Show table
spark.sql('SELECT * FROM name_goes_here').show()

## Output to .json file
spark.sql('SELECT * FROM name_goes_here').coalesce(1).write.format('json').option('header','false').save('/Users/YuanChunLin/HW5_1.json')


# Task 2
## Repeat task 1 with overlap window by 15 seconds

agg2 = df.withWatermark("timestamp", "5 seconds").groupBy(window(df.timestamp, "30 seconds", "15 seconds")).sum()

myquery2=agg2.writeStream.outputMode('update').format('memory').trigger(processingTime='2 seconds').queryName('name_goes_here_2').start()
myquery2.stop()

spark.sql('SELECT * FROM name_goes_here_2').show()

spark.sql('SELECT * FROM name_goes_here_2').coalesce(1).write.format('json').option('header','false').save('/Users/YuanChunLin/HW5_2.json')

