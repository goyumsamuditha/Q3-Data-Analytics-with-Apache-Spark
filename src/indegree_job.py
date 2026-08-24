import time
import os
import urllib.request
import gzip
import shutil
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, split
from pyspark import SparkFiles

def download_and_extract(raw_path:str):
    """
    Download and extract the dataset if it does not exist.
    """
    url = "https://snap.stanford.edu/data/web-BerkStan.txt.gz"
    path = raw_path + ".gz"
    
    if not os.path.exists(raw_path):
        print(f"Downloading dataset from {url}...")
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        urllib.request.urlretrieve(url, path)
        print(f"Download complete. Extracting {path}...")
        with gzip.open(path, 'rb') as f_in:
            with open(raw_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Extraction complete. Dataset saved to {raw_path}.")
    else:
        print(f"Dataset already exists at {raw_path}. Skipping download.")


def load_edges(spark: SparkSession, raw_path: str):
    """
    Load edges from the dataset into a DataFrame.
    """
    
    raw_edges_df = spark.read.text(raw_path)
    edges_df = raw_edges_df.filter(~col("value").startswith("#"))
    split_col = split(col("value"), r"\s+")
    
    return( edges_df.select(
        split_col.getItem(0).cast("long").alias("src"),
        split_col.getItem(1).cast("long").alias("dst"),
    )
    .na.drop()  # Drop rows with null values in src or dst
    )
    
def compute_indegree(edges_df):
    """
    Compute the indegree for each node.
    """
    indegree_df = edges_df.groupBy("dst").agg(count("src").alias("indegree")).orderBy(desc("indegree"))
    indegree_df.cache()  # Cache the DataFrame to optimize performance for subsequent actions
    top50_indegree_df = indegree_df.limit(50)
    return top50_indegree_df, indegree_df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    # Download and extract the dataset if it does not exist
    download_and_extract(args.input)
    
    # Build Spark session
    spark = SparkSession.builder.appName("IndegreeJob").config("spark.sql.shuffle.partitions", "8").getOrCreate()
    
    # Load edges from the dataset
    edges_df = load_edges(spark, args.input)
    edges_df.cache()  # Cache the DataFrame to optimize performance for subsequent actions
    
    # Compute indegree
    top50_indegree_df, indegree_df = compute_indegree(edges_df)
    
    # Save the results to the output path
    top50_indegree_df.coalesce(1).write.mode("overwrite").csv(args.output + "/top50_indegree", header=True)

    
    print(f"Top 50 indegree nodes saved to {args.output}/top50_indegree")
    top50_indegree_df.show(50, truncate=False)  # Show the top 50 indegree nodes in the console
    
    print("\n[SUCCESS] Keeping the Spark UI alive for 5 minutes so you can take screenshots!")
    print("Open your browser to http://localhost:4040 right now.")
    time.sleep(300)
    
    spark.stop()

if __name__ == "__main__":
    main()