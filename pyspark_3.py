import pyspark 
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os
import sys
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.csv("airports.csv", inferSchema = True, header = True )

c=str(sys.argv[1])
outp=str(sys.argv[2])

os.environ["SPARK_WORKER_CORES"]=c
os.system("echo $SPARK_WORKER_CORES")

orig_stdout = sys.stdout
f = open(outp, 'w')
sys.stdout = f

df.filter(df["LATITUDE"] >= '10').filter(df["LATITUDE"] <= '90').filter(df["LONGITUDE"] <= '-10').filter(df["LONGITUDE"] >= '-90').select('NAME').show(20000)
sys.stdout = orig_stdout
f.close()


print("number of CPUs used:",c) 
print("output written to:",outp)
