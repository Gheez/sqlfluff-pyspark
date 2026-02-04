df1 = spark.sql("SELECT * FROM table1")
df2 = spark.sql("SELECT * FROM table2")
result = df1.join(df2, "id")
