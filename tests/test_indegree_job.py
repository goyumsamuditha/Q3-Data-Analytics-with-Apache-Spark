import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from src.indegree_job import compute_indegree

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("IndegreeJobTest").getOrCreate()

def test_compute_indegree(spark):
    schema = StructType([
        StructField("src", LongType(), True),
        StructField("dst", LongType(), True)
    ])
    mock_data = [
        (1, 2),
        (2, 3),
        (3, 2),
        (4, 3),
        (5, 2)
    ]
    edges_df = spark.createDataFrame(mock_data, schema)
    top50_indegree_df, indegree_df = compute_indegree(edges_df)
    results = top50_indegree_df.collect()
    
    assert len(results) == 3 # There are 3 unique destination nodes in the mock data
    assert results[0]["dst"] == 2 and results[0]["indegree"] == 3 # Node 2 has an indegree of 3