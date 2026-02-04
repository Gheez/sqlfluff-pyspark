# Expected preserved elements after SQL formatting
PRESERVED_ELEMENTS = [
    "import pandas as pd",
    "from pyspark.sql import SparkSession",
    "spark = SparkSession.builder",
    "# First query",
    "# Second query",
    "# Process results",
    "df1 = spark.sql",
    "df2 = spark.sql",
    ".filter(",
    "result = df1.join",
    "result.show()",
]
