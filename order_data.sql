-- build the aggreate cmf data associated with the order record
WITH order_record_cmf_data AS (
	SELECT
	o_r.record_id AS order_record_id,
	count(cmf.id) AS count_cmf,
	count(DISTINCT cmf.fund_code) AS count_distinct_cmf_fund_codes,
	sum(cmf.copies) AS sum_cmf_copies,
	json_agg(
		json_build_object(
			'display_order', cmf.display_order,
			'cmf_id', cmf.id,
			'fund_code_num', fm.code_num,
			'fund_code', fm.code,
			'acct_unit_code_num', au.code_num,
			'copies', cmf.copies,
			'location_code', cmf.location_code
		)
		ORDER BY
		cmf.display_order ASC
	) AS cmf_data
	FROM 
	sierra_view.order_record AS o_r
	LEFT OUTER JOIN sierra_view.order_record_cmf AS cmf ON cmf.order_record_id = o_r.record_id
	-- it's unfortunate, but it seems like the code number can have TEXT values like 'none' for example..
	-- so, it's necessary to filter those out with a regex
	LEFT OUTER JOIN sierra_view.fund_master AS fm ON fm.code_num = NULLIF(regexp_replace(cmf.fund_code, '[^0-9]*', '', 'g'),'')::int
	LEFT OUTER JOIN sierra_view.accounting_unit AS au ON au.id = fm.accounting_unit_id 
	WHERE 
	cmf.location_code != 'multi'
	GROUP BY 1
)
SELECT
-- build order record data
rm.record_num AS order_record_num,
d.*,
brorl.orders_display_order,
brorl.bib_record_id  AS bib_record_id,
rm.creation_date_gmt,
order_record.accounting_unit_code_num,
(order_record.estimated_price * 100.0) :: INTEGER AS estimated_price_cents,
order_record.form_code AS physical_form_code,
fpn."name" AS physical_form_name,
order_record.order_date_gmt,
order_record.catalog_date_gmt,
order_record.order_type_code,
otpm."name" AS order_type_name,
order_record.received_date_gmt,
order_record.receiving_location_code,
order_record.order_status_code,
ospn."name" AS order_status_name,
order_record.vendor_record_code,
order_record.volume_count,
vr.code AS vendor_record_code,
vr.record_id AS vendor_record_id
FROM 
order_record_cmf_data AS d
LEFT OUTER JOIN sierra_view.order_record AS order_record ON order_record.record_id = d.order_record_id
LEFT OUTER JOIN sierra_view.record_metadata AS rm ON rm.id = order_record.record_id
LEFT OUTER JOIN sierra_view.bib_record_order_record_link AS brorl ON brorl.order_record_id = order_record.record_id	
LEFT OUTER JOIN sierra_view.form_property AS fp ON fp.code = order_record.form_code
LEFT OUTER JOIN sierra_view.form_property_name AS fpn ON fpn.form_property_id = fp.id 
LEFT OUTER JOIN sierra_view.order_type_property_myuser AS otpm ON otpm.code = order_record.order_type_code
LEFT OUTER JOIN sierra_view.order_status_property AS osp ON osp.code = order_record.order_status_code 
LEFT OUTER JOIN sierra_view.order_status_property_name AS ospn ON ospn.order_status_property_id = osp.id
LEFT OUTER JOIN sierra_view.vendor_record AS vr ON vr.code = order_record.vendor_record_code