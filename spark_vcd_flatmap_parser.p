# Optimized VCD (Value Change Dump) file processor using PySpark
# Features:
# - Parallel processing of multiple VCD files
# - Memory-efficient parsing
# - Structured output with schema enforcement
# - Partitioned Parquet output for efficient storage

import os
import time
import resource
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    """Main execution function for the VCD processing pipeline."""
    
    #  SETUP
    
    # Start timing for performance measurement
    start_wall = time.time()
    start_cpu = resource.getrusage(resource.RUSAGE_SELF)

    # Configure Spark session with optimization settings
    spark = SparkSession.builder \
        .appName("Optimized VCD Parser") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()

    # Get Spark context and set log level to reduce noise
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # *********************
    # * SCHEMA DEFINITION
    # *********************
    
    # Define the output schema for our parsed VCD data
    schema = StructType([
        StructField("file", StringType(), False),    # Source filename
        StructField("signal", StringType(), False),    # Signal name
        StructField("tick", IntegerType(), False),     # Simulation time tick
        StructField("value", StringType(), False)     # Signal value at this tick
    ])

    # *********************
    # * PARSING FUNCTIONS
    # *********************
    
    def parse_vcd(vcd_str, filename):
        """
        Parse a VCD file content string into signal change events.
        
        Args:
            vcd_str: Complete content of a VCD file as string
            filename: Name of the source file for tracking
            
        Returns:
            Tuples of (filename, signal_name, tick, value) for each signal change
        """
        lines = vcd_str.splitlines()
        vars = {}  # Dictionary mapping VCD encodings to signal names
        started = False  # Flag for when we're past the header
        tick = 0  # Current simulation time
        
        # First pass: extract variable definitions from header
        for line in lines:
            if "$enddefinitions" in line:
                break
            if "var" in line:
                parts = line.split()
                if len(parts) >= 5:  # Ensure we have complete var definition
                    vars[parts[3]] = parts[4]  # Map encoding to signal name
        
        # Second pass: process value changes with minimal memory usage
        prev_values = {}  # Track previous values to only emit changes
        for line in lines:
            line = line.strip()
            
            # Check for dumpvars marker indicating start of data
            if "$dumpvars" in line:
                started = True
                prev_values = {enc: "-1" for enc in vars}  # Initialize all to -1
                continue
                
            if not started:
                continue  # Skip until we reach the data section
                
            # Handle time markers
            if line.startswith("#"):
                tick = int(line[1:])  # Extract time value
                
            # Handle signal value changes
            elif line:
                # Single-bit value (0,1,x,X,z,Z)
                if line[0] in '01xXzZ':
                    val = line[0]
                    enc = line[1:]
                    if enc in vars and prev_values.get(enc) != val:
                        prev_values[enc] = val
                        yield (filename, vars[enc], tick, val)
                        
                # Multi-bit value (starts with b)
                elif ' ' in line:
                    val, enc = line[1:].split(' ', 1)
                    if enc in vars and prev_values.get(enc) != val:
                        prev_values[enc] = val
                        yield (filename, vars[enc], tick, val)

    def parse_partition(files):
        """
        Spark partition processor that handles a set of files.
        
        Args:
            files: Iterable of (filepath, content) tuples
            
        Yields:
            Parsed VCD records from all files in the partition
        """
        for path, content in files:
            try:
                filename = os.path.basename(path)
                yield from parse_vcd(content, filename)
            except Exception as e:
                print(f"Error processing {path}: {str(e)}")

    
    #DATA PROCESSING
    
    # Load all VCD files with dynamic partitioning
    vcd_dir = "all_vcds"
    input_rdd = sc.wholeTextFiles(
        os.path.join(vcd_dir, "*.vcd"), 
        minPartitions=sc.defaultParallelism * 2  # Use 2x parallelism for better load balancing
    )
    
    # Process files and create DataFrame with our schema
    df = spark.createDataFrame(
        input_rdd.mapPartitions(parse_partition),
        schema=schema
    ).cache()  # Cache for multiple operations

    # Show sample of the parsed data
    print("\nSample of parsed VCD data:")
    df.show(10, truncate=False)

    # OUTPUT
    
    # Write output in partitioned Parquet format for efficient storage
    df.write.mode("overwrite") \
        .partitionBy("file") \
        .option("compression", "snappy") \
        .parquet("vcd_output_parquet")

    # *********************
    # * CLEANUP & METRICS
    # *********************
    
    # Capture and print timing statistics
    end_wall = time.time()
    end_cpu = resource.getrusage(resource.RUSAGE_SELF)
    print("\nOptimized Spark VCD parse timing:")
    print(f"Wall time\t{end_wall - start_wall:.3f}s")
    print(f"User CPU\t{end_cpu.ru_utime - start_cpu.ru_utime:.3f}s")
    print(f"System CPU\t{end_cpu.ru_stime - start_cpu.ru_stime:.3f}s")

    # Clean up Spark session
    spark.stop()

if __name__ == "__main__":
    main()
