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
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    old_hist_recs