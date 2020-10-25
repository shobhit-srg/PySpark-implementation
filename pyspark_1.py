import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os
import sys
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

c=str(sys.argv[1])
outp=str(sys.argv[2])
os.environ["SPARK_WORKER_CORES"]=c
os.system("echo $SPARK_WORKER_CORES")

df = spark.read.csv("airports.csv", inferSchema = True, header = True )
df2 = df.groupBy("COUNTRY").count()

df2.write.save('a.csv', format='csv')
os.system('cat a.csv/*.csv > answer1.csv')
df1=spark.read.csv("answer1.csv", inferSchema = True, header = False)
df1.withColumnRenamed("_c0","COUNTRY")
df1.withColumnRenamed("_c1","COUNT")

df11 = df1.withColumnRenamed("_c0","COUNTRY").withColumnRenamed("_c1","COUNT")

orig_stdout = sys.stdout
f = open(outp, 'w')
sys.stdout = f

df11.show(250)

sys.stdout = orig_stdout
f.close()

print("number of CPUs used:",c) 
print("output written to:",outp)

