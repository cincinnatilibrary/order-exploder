select
  order_record_num,
  date(creation_date_gmt) as creation_date,
  -- order_record_id,
  count_cmf,
  count_distinct_cmf_fund_codes as "count fund codes",
  sum_cmf_copies,
  cmf_data,
  -- orders_display_order,
  bib_record_id,
  accounting_unit_code_num as "accnt unit code",
  estimated_price_cents / 100.0 as estimated_price,
  -- physical_form_code,
  physical_form_name,
  date(order_date_gmt) as order_date,
  date(catalog_date_gmt) as cat_date,
  -- order_type_code,
  order_type_name,
  -- received_date_gmt,
  -- receiving_location_code,
  order_status_code,
  order_status_name,
  vendor_record_code,
  vendor_record_id,
  volume_count
from
  orders
where
  "count_distinct_cmf_fund_codes" > :count_gt_funds_codes_used
  and "order_status_code" = :order_status_code
order by
  creation_date
