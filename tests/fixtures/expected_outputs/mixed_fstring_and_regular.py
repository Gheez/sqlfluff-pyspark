PRESERVED_ELEMENTS = [
    "# Regular string",
    'table_name = "users"',
    "df1 = spark.sql",
    "# F-string",
    "user_id = 123",
    "df2 = spark.sql",
    "# Another regular string",
    "df3 = spark.sql",
    "# Combine",
    "result = df1.union",
    "result.show()",
]

# F-string expressions should be preserved
FSTRING_EXPRESSIONS = [
    "{table_name}",
    "table_name",
    "{user_id}",
    "user_id",
]
