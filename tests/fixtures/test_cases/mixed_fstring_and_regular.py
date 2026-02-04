# Regular string
table_name = "users"
df1 = spark.sql("SELECT * FROM users WHERE active = 1")

# F-string
user_id = 123
df2 = spark.sql(f"SELECT * FROM {table_name} WHERE id = {user_id}")

# Another regular string
df3 = spark.sql("SELECT COUNT(*) as total FROM users")

# Combine
result = df1.union(df2).union(df3)
result.show()
