import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys
import os
import pandas as pd

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

c=str(sys.argv[1])
outp=str(sys.argv[2])
os.environ["SPARK_WORKER_CORES"]=c
os.system("echo $SPARK_WORKER_CORES")


df1=spark.read.csv("answer1.csv", inferSchema = True, header = False)
orig_stdout = sys.stdout
f = open(outp, 'w')
sys.stdout = f

df1.orderBy(df1["_c1"].desc()).drop("_c1").show(1)

sys.stdout = orig_stdout
f.close()



print("number of CPUs used:",c) 
print("output written to:",outp)
