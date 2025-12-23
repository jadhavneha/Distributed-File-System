import sys
import time
from pyspark import SparkContext

# ==========================================
# CONFIGURATION
# ==========================================
# IMPORTANT: Before running, you must distribute the CORRECT CSV
# to /tmp/spark_input/data.csv on all workers!
# For cus_* apps -> copy customers.csv
# For gis_* apps -> copy champaign_signs_big.csv
INPUT_PATH = "file:///tmp/spark_input/*.csv"

# CHANGE THIS to switch experiments:
# "cus_replace", "cus_aggregate", "gis_warning", "gis_school"
APP_TYPE = "gis_school"

def main():
    sc = SparkContext(appName="RainStormBenchmark")
    sc.setLogLevel("WARN")

    print(f"--- STARTING BATCH BENCHMARK FOR {APP_TYPE} ---")

    # 1. Start Timer
    start_time = time.time()

    # 2. Load Data
    try:
        lines = sc.textFile(INPUT_PATH)
        # Verify files exist
        first_line = lines.first()
    except:
        print(f"ERROR: No files found at {INPUT_PATH}")
        print("Please run your scp loop to put the correct CSV in /tmp/spark_input/")
        return

    # 3. Define Logic
    output_rdd = None

    # ---------------------------------------------------------
    # TEST 1: Customers - Filter & Transform
    # RainStorm: grep:India replace:India:Bharat
    # ---------------------------------------------------------
    if APP_TYPE == "cus_replace":
        # Stage 1: Filter lines containing "India"
        filtered = lines.filter(lambda line: "India" in line)
        # Stage 2: Replace string
        output_rdd = filtered.map(lambda line: line.replace("India", "Bharat"))

    # ---------------------------------------------------------
    # TEST 2: Customers - Filter & Aggregate
    # RainStorm: grep:LLC aggregateByKeys:7
    # ---------------------------------------------------------
    elif APP_TYPE == "cus_aggregate":
        # Stage 1: Filter lines containing "LLC"
        filtered = lines.filter(lambda line: "LLC" in line)

        # Stage 2: Group by Column 7 (Index 6)
        def map_to_col7(line):
            parts = line.split(",")
            # RainStorm arg 7 means index 6
            if len(parts) > 6:
                return (parts[6], 1)
            return None

        mapped = filtered.map(map_to_col7).filter(lambda x: x is not None)
        output_rdd = mapped.reduceByKey(lambda a, b: a + b)

    # ---------------------------------------------------------
    # TEST 3: GIS - Filter (Identity)
    # RainStorm: grep:Warning identity
    # ---------------------------------------------------------
    elif APP_TYPE == "gis_warning":
        # Stage 1: Filter lines containing "Warning"
        # Stage 2: Identity (Pass through) -> Spark implicitly does this with filter result
        output_rdd = lines.filter(lambda line: "Warning" in line)

    # ---------------------------------------------------------
    # TEST 4: GIS - Filter & Aggregate
    # RainStorm: grep:School aggregateByKeys:7
    # ---------------------------------------------------------
    elif APP_TYPE == "gis_school":
        # Stage 1: Filter lines containing "School"
        filtered = lines.filter(lambda line: "School" in line)

        # Stage 2: Group by Column 7 (Index 6)
        def map_to_col7(line):
            parts = line.split(",")
            if len(parts) > 6:
                return (parts[6], 1)
            return None

        mapped = filtered.map(map_to_col7).filter(lambda x: x is not None)
        output_rdd = mapped.reduceByKey(lambda a, b: a + b)

    # 4. Force Execution
    result_count = output_rdd.count()

    # 5. Stop Timer
    end_time = time.time()
    duration = end_time - start_time

    print(f"--- RESULT: {result_count} records processed ---")
    print(f"--- DURATION: {duration:.4f} seconds ---")
    print(f"--- THROUGHPUT: {result_count / duration:.2f} records/sec ---")

if __name__ == "__main__":
    main()