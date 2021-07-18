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
          LEFT OUTER JOIN merge_keys k
              ON k.customer_dim_key = s.customer_dim_key
 WHERE    k.customer_dim_key IS  NULL