from pyspark.sql import SparkSession
import time

def main():
    print("=" * 60)
    print(" SPARK SIMPLE COUNTER JOB")
    print("=" * 60)

    spark = SparkSession.builder \
            .appName("SimpleCounter")\
            .getOrCreate()
    
    sc = spark.sparkContext()
    sc.setLogLevel("WARN")

    print(f"\n [INFO] Spark Version: {spark.version}")
    print(f"[INDO] Application ID: {sc.applicationId}")
    print(f"[INFO] Master: {sc.master}")

    start_time = time.time()
    num_elements = 1_000_000
    num_partitions = 10
    
    print(f"\n [STEP 1] Creating RDD with {num_elements:,} elements across {num_partitions} partitions")
    numbers_rdd = sc.parallelize(range(1, num_elements + 1), num_partitions)

    print("[STEP 2] Computing statistics in parallel...")
    count = numbers_rdd.count()
    print(f" - Count: {count:,}")
    
    total_sum = numbers_rdd.sum()
    print(f" - Sum: {total_sum:}")
    
    average = total_sum/count
    print(f" - Average: {average:,.2f}")

    min_val = numbers_rdd.min()
    max_val = numbers_rdd.max()
    print(f" - Min: {min_val:,}")
    print(f" - Max: {max_val:,}")
    
    print(f"[STEP 3] Creating Dataframe and performing aggregations...")
    df = spark.createDataFrame([
        (i, i*2, 'even' if i % 2 == 0 else 'odd') for i in range(1, 101)],
        ['number', 'doubled', 'type']
        )
    
    print("\n [INFO] Sample Dataframe:")
    df.show(5)

    print(' - Aggregation by "type": ')
    df.groupBy("type").agg(
        {'number': 'sum',
        'doubled': 'avg'}).show()
    
    elapsed_time = time.time() - start_time
    

    print("=" * 60)
    print(" SPARK SIMPLE COUNTER JOB COMPLETE")
    print(f"\n [INFO] Job completed in {elapsed_time:.2f} seconds")

    spark.stop()

if __name__ == "__main__":
    main()