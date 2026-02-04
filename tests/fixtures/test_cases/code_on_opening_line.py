def get_data():
    df = spark.sql("SELECT * FROM users WHERE active = 1")
    return df
