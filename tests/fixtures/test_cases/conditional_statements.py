if use_new_table:
    df = spark.sql("SELECT * FROM new_table WHERE id > 100")
else:
    df = spark.sql("SELECT * FROM old_table WHERE id > 100")

df.show()
