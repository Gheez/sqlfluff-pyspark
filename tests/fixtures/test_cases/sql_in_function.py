def process_data():
    # Initialize
    spark_session = get_spark_session()  # noqa: F841

    # Execute query
    result = spark.sql("""
        SELECT 
            id,
            name,
            email
        FROM users
        WHERE active = 1
    """)

    # Process results
    return result.collect()
