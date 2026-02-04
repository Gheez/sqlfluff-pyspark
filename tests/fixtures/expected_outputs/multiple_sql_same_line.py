PRESERVED_ELEMENTS = [
    "df1 = spark.sql",
    "df2 = spark.sql",
    "result = df1.join",
]

# At least one of these should be present (SQL content)
SQL_CONTENT_HINTS = [
    "table1",
    "table2",
    "SELECT",
]
