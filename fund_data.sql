SELECT 
fm.code_num AS fund_code_num,
fm.code AS fund_code,
au.code_num AS accounting_unit_code_num,
efpn."name" AS external_fund_name,
fund.fund_type, -- code that determines the fund property -- fbal:     data is for a current fund.
																													-- oldfbal:  data is for a fund from last year.
																													-- fprevbal: data is for a Fund Activity Report.
fund.appropriation,
fund.expenditure,
fund.encumbrance,
fund.num_orders,
fund.num_payments,
fund.warning_percent
FROM 
sierra_view.fund_master AS fm 
JOIN sierra_view.accounting_unit AS au ON au.id = fm.accounting_unit_id 
JOIN sierra_view.external_fund_property AS efp ON efp.code_num = fm.code_num 
JOIN sierra_view.external_fund_property_name AS efpn ON efpn.external_fund_property_id = efp.id
JOIN sierra_view.fund AS fund ON fund.fund_code = fm.code 
--JOIN sierra_view.fund AS fund ON fund.fund_code = fm.code
--JOIN sierra_view.external_fund_property AS efp ON efp.code_num = fm.code_num 
--JOIN sierra_view.external_fund_property_name AS efpn ON efpn.external_fund_property_id = efp.id 
--SELECT * FROM sierra_view.external_fund_property
ORDER BY
fund_code_num