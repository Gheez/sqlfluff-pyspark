PRESERVED_ELEMENTS = [
    "if use_new_table:",
    "df = spark.sql",
    "else:",
    "df.show()",
]

# At least one of these should be present (SQL content)
SQL_CONTENT_HINTS = [
    "new_table",
    "old_table",
    "SELECT",
]
