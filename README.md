# vcd2df-spark

Spark-native parser for VCD (Value Change Dump) files. Converts RTL simulation traces into structured DataFrames for scalable analysis.

## Features

- Parses VCDs in parallel using Spark
- Emits (file, signal, tick, value) rows
- Writes output as partitioned Parquet
- Supports distributed filtering and SQL queries

## Output Schema

- `file`: source VCD filename  
- `signal`: register or wire name  
- `tick`: simulation time as int  
- `value`: signal value ('0', '1', etc.)

## Assumptions

- Signals with the same encoded name across modules are treated as equivalent if their values do not diverge
- No reconstruction of module hierarchy; signal names are treated as flat strings
- Non-numeric values (e.g. 'x', 'z') are mapped to `-1` for compatibility
- Only value **changes** are recorded; unchanged signals between timestamps are omitted
