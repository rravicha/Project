 SELECT   t.customer_dim_key
 FROM     current_scd2 t
          LEFT OUTER JOIN customer_data s
              ON t.customer_number = s.customer_number
 WHERE    s.customer_number IS NULL
          AND t.is_current = True