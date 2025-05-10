# adapted from https://github.com/vcd2df/py
# use a generator over a split (still lazy) instead of file ptr

# on my device, /home/user/.local/bin/spark-submit script.py 

def get_vars(lines):
    line = next(lines)
    vars = {} # insertion order >= 3.7
    while "$enddefinitions" not in line:
        if "var" in line:
            parts = line.split()
            if parts[4] not in vars.values():
                vars[parts[3]] = parts[4]
        line = next(lines)
    return vars

def str2df(str):
    lines = (line for line in str.splitlines())
    vars = get_vars(lines)
    names = vars.copy()
    vars = {var:-1 for var in vars.keys()}
    df = {}
    while "$dumpvars" not in next(lines):
        pass
    time = "#0"
    for line in lines:
        if "#" in line[0]: # Check for tick
            df[time] = pd.Series(vars.values())
            time = line.strip()
        else: # Else two cases, words and bits
            if " " in line: # word
                val, var = line[1:].strip().split()
            else: # bit
                val, var = line[0], line[1:].strip()
            if var in vars:
                vars[var] = int(val, 2) if val.isdigit() else -1
    df = pd.DataFrame(df, dtype=int)
    df.index = names.values()
    return df
        
# "shadow" registers contain iflow status
# find changes from 0 to 1 at nonzero times
def iflow_times(local):
    local = local[local.index.str.contains("shadow")]
    local = local[local.any(axis=1)]
    local = local.idxmax(axis=1)
    local = local[local != "#0"]
    local = local.apply(lambda s : int(s[1:]))
    return local

import pandas as pd
import pickle
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL vcd2df iflow demo").getOrCreate()

from pyspark.sql.functions import col, udf, input_file_name

# VCDs from here: https://github.com/cd-public/Isadora/tree/master/model/single/vcds
# Set path appropriately, perhaps 
df = spark.read.text("vcds/*.vcd", wholetext=True).withColumn("filename", input_file_name())
df = df.select(col("value"), udf(lambda fn : fn.split("/")[-1].split(".")[0])(col("filename")).alias("src"))
# switch from udf to rdd map to not wrangle schema
mid = df.rdd.map(lambda x: {x[1].replace("shadow_",""):iflow_times(str2df(x[0]))})
xs = [x for x in mid.collect() if not list(x.values())[0].empty] # reduce stage
pickle.dump(xs, open("iflow_times.pkl", "w"))
spark.stop()
