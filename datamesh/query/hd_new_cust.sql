SELECT   s.customer_number,
          s.first_name,
          s.last_name,
          s.middle_initial,
          s.address,
          s.city,
          s.state,
          s.zip_code,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'CST')) 
              AS eff_start_date,
          DATE('9999-12-31') AS eff_end_date,
          BOOLEAN(1) AS is_current
 FROM     customer_data s
          LEFT OUTER JOIN current_scd2 t
              ON t.customer_number = s.customer_number
 WHERE    t.customer_number IS NULL