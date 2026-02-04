# Query with code on same lines
df = spark.sql("""SELECT * FROM users""").filter(
    "active = 1"
)  # Single line triple quote
result = df.collect()  # Get all results

# Multi-line with code
df2 = spark.sql("""
    SELECT id, name FROM products
    WHERE price > 100
""").orderBy("price")  # Order by price
