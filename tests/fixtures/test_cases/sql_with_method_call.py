spark.sql("""
    SELECT distinct  
        '{V_COMPANY_CODE_CARSTORE_AS}'      AS COMPANY_CODE
        ,IFNULL(b.GL_ACCOUNT_NO_MAP,a.No_)  AS ACCOUNT_NUMBER
        ,a.Name                             AS ACCOUNT_NAME
    FROM
        HEDIN_HAAS.EXTR_CARSTORE_AS_G_L_ACCOUNT a
        LEFT JOIN hedin_automotive_carstore_as_g_l_entry b on a.No_ = b.GL_ACCOUNT_NO_ORG

 """).createOrReplaceTempView("Carstore_AS_GLA")
