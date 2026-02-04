# First query
query1 = spark.sql("SELECT * FROM table1")
df1 = query1.toPandas()  # Convert to pandas

# Second query
query2 = spark.sql("SELECT * FROM table2 WHERE date > '2024-01-01'")
df2 = query2.toPandas()

# Combine results
combined = pd.concat([df1, df2])
