spark.sql("SELECT id, name FROM users").createOrReplaceTempView("users_view")
print("Done")
