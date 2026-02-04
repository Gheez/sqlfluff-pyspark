# Setup
df = spark.sql("""
    SELECT * FROM table
    WHERE id > 100
""").filter("status = 'active'")  # Filter active records
result = df.collect()  # Get results
