import os
import time
import resource
from pyspark.sql import SparkSession, Row

# Timing (real, user, sys)
start_wall = time.time()
start_cpu = resource.getrusage(resource.RUSAGE_SELF)

# Initialize Spark
spark = SparkSession.builder \
    .appName("Improved Pure Spark VCD Parser") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# Parse VCD header for variable definitions
def get_vars(lines):
    vars = {}
    for line in lines:
        if "$enddefinitions" in line:
            break
        if "var" in line:
            parts = line.split()
            if len(parts) >= 5 and parts[4] not in vars.values():
                vars[parts[3]] = parts[4]
    return vars

# Convert full VCD string into a list of Rows for Spark
def parse_vcd_to_rows(vcd_str, filename):
    lines = vcd_str.splitlines()
    vars = get_vars(lines)
    keys = vars.keys()
    signal_state = {key: "-1" for key in keys}
    rows = []

    time_marker = 0
    parsing_started = False

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if "$dumpvars" in line:
            parsing_started = True
            continue
        if not parsing_started:
            continue
        if line.startswith("#"):
            time_marker = int(line[1:])
        elif " " in line:
            value, key = line.split()
            if key in signal_state:
                signal_state[key] = value
        elif len(line) > 1:
            value = line[0]
            key = line[1:]
            if key in signal_state:
                signal_state[key] = value
        row_data = {"filename": filename, "time": time_marker}
        row_data.update(signal_state.copy())
        rows.append(Row(**row_data))
    return rows

# Load all VCD files as (filename, content)
vcd_dir = "vcds"  # Update this path to where your VCDs are stored
rdd = sc.wholeTextFiles(vcd_dir).flatMap(lambda f: parse_vcd_to_rows(f[1], os.path.basename(f[0])))

# Create DataFrame
df = spark.createDataFrame(rdd)

# Show schema and sample output
df.printSchema()
df.show(5)

# Timing results
end_wall = time.time()
end_cpu = resource.getrusage(resource.RUSAGE_SELF)

print("\nSpark VCD parse timing:")
print("real\t{:.3f}s".format(end_wall - start_wall))
print("user\t{:.3f}s".format(end_cpu.ru_utime - start_cpu.ru_utime))
print("sys \t{:.3f}s".format(end_cpu.ru_stime - start_cpu.ru_stime))

spark.stop()
