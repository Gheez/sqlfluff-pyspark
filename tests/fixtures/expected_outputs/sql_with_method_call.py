PRESERVED_ELEMENTS = [
    ".createOrReplaceTempView",
    '"Carstore_AS_GLA"',
]

# Method call should be complete (not truncated)
METHOD_CALL_CHECK = {
    "method": ".createOrReplaceTempView",
    "min_length": len(".createOrReplaceTempView"),
}
