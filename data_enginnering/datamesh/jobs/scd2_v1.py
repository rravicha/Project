
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("scd2_demo").getOrCreate()
# v_s3_path = "s3://mybucket/dim_customer_scd"


# In[2]:


hd_current_scd2 = """
 SELECT   BIGINT(1) AS customer_dim_key,
          STRING('John') AS first_name,
          STRING('Smith') AS last_name,
          STRING('G') AS middle_initial,
          STRING('123 Main Street') AS address,
          STRING('Springville') AS city,
          STRING('VT') AS state,
          STRING('01234-5678') AS zip_code,
          BIGINT(289374) AS customer_number,
          DATE('2014-01-01') AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 UNION
 SELECT   BIGINT(2) AS customer_dim_key,
          STRING('Susan') AS first_name,
          STRING('Jones') AS last_name,
          STRING('L') AS middle_initial,
          STRING('987 Central Avenue') AS address,
          STRING('Central City') AS city,
          STRING('MO') AS state,
          STRING('49257-2657') AS zip_code,
          BIGINT(862447) AS customer_number,
          DATE('2015-03-23') AS eff_start_date,
          DATE('2018-11-17') AS eff_end_date,
          BOOLEAN(0) AS is_current
 UNION
 SELECT   BIGINT(3) AS customer_dim_key,
          STRING('Susan') AS first_name,
          STRING('Harris') AS last_name,
          STRING('L') AS middle_initial,
          STRING('987 Central Avenue') AS address,
          STRING('Central City') AS city,
          STRING('MO') AS state,
          STRING('49257-2657') AS zip_code,
          BIGINT(862447) AS customer_number,
          DATE('2018-11-18') AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 UNION
 SELECT   BIGINT(4) AS customer_dim_key,
          STRING('William') AS first_name,
          STRING('Chase') AS last_name,
          STRING('X') AS middle_initial,
          STRING('57895 Sharp Way') AS address,
          STRING('Oldtown') AS city,
          STRING('CA') AS state,
          STRING('98554-1285') AS zip_code,
          BIGINT(31568) AS customer_number,
          DATE('2018-12-07') AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
"""


# In[4]:


df_current_scd2 = spark.sql(hd_current_scd2)


# In[54]:


df_current_scd2.coalesce(1).write.csv("/home/susi/data/current_scd2.csv")


# In[6]:


df_current_scd2.createOrReplaceTempView("current_scd2")


# In[8]:


hd_customer_data = """
 SELECT   BIGINT(289374) AS customer_number,
          STRING('John') AS first_name,
          STRING('Smith') AS last_name,
          STRING('G') AS middle_initial,
          STRING('456 Derry Court') AS address,
          STRING('Springville') AS city,
          STRING('VT') AS state,
          STRING('01234-5678') AS zip_code
 UNION
 SELECT   BIGINT(932574) AS customer_number,
          STRING('Lisa') AS first_name,
          STRING('Cohen') AS last_name,
          STRING('S') AS middle_initial,
          STRING('69846 Mason Road') AS address,
          STRING('Atlanta') AS city,
          STRING('GA') AS state,
          STRING('26584-3591') AS zip_code
 UNION
 SELECT   BIGINT(862447) AS customer_number,
          STRING('Susan') AS first_name,
          STRING('Harris') AS last_name,
          STRING('L') AS middle_initial,
          STRING('987 Central Avenue') AS address,
          STRING('Central City') AS city,
          STRING('MO') AS state,
          STRING('49257-2657') AS zip_code
 UNION
 SELECT   BIGINT(31568) AS customer_number,
          STRING('William') AS first_name,
          STRING('Chase') AS last_name,
          STRING('X') AS middle_initial,
          STRING('57895 Sharp Way') AS address,
          STRING('Oldtown') AS city,
          STRING('CA') AS state,
          STRING('98554-1285') AS zip_code
"""


# In[9]:


df_customer_data= spark.sql(hd_customer_data)
df_customer_data.createOrReplaceTempView("customer_data")


# In[10]:


df_customer_data.show()


# In[20]:


hd_new_curr_recs = """
 SELECT   t.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PST'))
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 FROM     customer_data s
          INNER JOIN current_scd2 t
              ON t.customer_number = s.customer_number
              AND t.is_current = True
 WHERE    NVL(s.first_name, '') <> NVL(t.first_name, '')
          OR NVL(s.last_name, '') <> NVL(t.last_name, '')
          OR NVL(s.middle_initial, '') <> NVL(t.middle_initial, '')
          OR NVL(s.address, '') <> NVL(t.address, '')
          OR NVL(s.city, '') <> NVL(t.city, '')
          OR NVL(s.state, '') <> NVL(t.state, '')
          OR NVL(s.zip_code, '') <> NVL(t.zip_code, '')
"""


# In[21]:


df_new_curr_recs = spark.sql(hd_new_curr_recs)


# In[22]:


df_new_curr_recs.createOrReplaceTempView("new_curr_recs")


# In[23]:


df_new_curr_recs.show()


# In[24]:


df_modfied_keys = df_new_curr_recs.select("customer_dim_key")


# In[25]:


df_modfied_keys.createOrReplaceTempView("modfied_keys")


# In[30]:


hd_new_hist_recs = """
 SELECT   t.customer_dim_key,
          t.customer_number,
          t.first_name,
          t.last_name,
          t.middle_initial,
          t.address,
          t.city,
          t.state,
          t.zip_code,
          t.eff_start_date,
          DATE_SUB(
              DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PST')), 1
          ) AS eff_end_date,
          BOOLEAN(0) AS is_current
 FROM     current_scd2 t
          INNER JOIN modfied_keys k
              ON k.customer_dim_key = t.customer_dim_key
 WHERE    t.is_current = True
"""


# In[31]:


df_new_hist_recs = spark.sql(hd_new_hist_recs)


# In[32]:


df_new_hist_recs.createOrReplaceTempView("new_hist_recs")


# In[33]:


df_new_hist_recs.show()


# In[34]:


hd_unaffected_recs = """
 SELECT   s.customer_dim_key,
          s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          s.eff_start_date,
          s.eff_end_date,
          s.is_current
 FROM     current_scd2 s
          LEFT OUTER JOIN modfied_keys k
              ON k.customer_dim_key = s.customer_dim_key
 WHERE    k.customer_dim_key IS NULL
"""


# In[35]:


df_unaffected_recs = spark.sql(hd_unaffected_recs)


# In[36]:


df_unaffected_recs.createOrReplaceTempView("unaffected_recs")


# In[37]:


df_unaffected_recs.show()


# In[42]:


hd_new_cust = """
 SELECT   s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'PST')) 
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 FROM     customer_data s
          LEFT OUTER JOIN current_scd2 t
              ON t.customer_number = s.customer_number
 WHERE    t.customer_number IS NULL
"""


# In[43]:


df_new_cust = spark.sql(hd_new_cust)


# In[44]:


df_new_cust.createOrReplaceTempView("new_cust")


# In[45]:


df_new_cust.show()


# In[46]:


v_max_key = spark.sql(
    "SELECT STRING(MAX(customer_dim_key)) FROM current_scd2"
).collect()[0][0]


# In[47]:


v_max_key


# In[48]:


hd_new_scd2 = """
 WITH a_cte
 AS   (
        SELECT     x.first_name, x.last_name,
                   x.middle_initial, x.address,
                   x.city, x.state, x.zip_code,
                   x.customer_number, x.eff_start_date,
                   x.eff_end_date, x.is_current
        FROM       new_cust x
        UNION ALL
        SELECT     y.first_name, y.last_name,
                   y.middle_initial, y.address,
                   y.city, y.state, y.zip_code,
                   y.customer_number, y.eff_start_date,
                   y.eff_end_date, y.is_current
        FROM       new_curr_recs y
      )
  ,   b_cte
  AS  (
        SELECT  ROW_NUMBER() OVER(ORDER BY a.eff_start_date)
                    + BIGINT('{v_max_key}') AS customer_dim_key,
                a.first_name, a.last_name,
                a.middle_initial, a.address,
                a.city, a.state, a.zip_code,
                a.customer_number, a.eff_start_date,
                a.eff_end_date, a.is_current
        FROM    a_cte a
      )
  SELECT  customer_dim_key, first_name, last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    b_cte
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    unaffected_recs
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    new_hist_recs
"""


# In[49]:


df_new_scd2 = spark.sql(hd_new_scd2.replace("{v_max_key}", v_max_key))


# In[50]:


df_new_scd2.show()

