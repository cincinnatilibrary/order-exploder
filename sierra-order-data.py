#!/usr/bin/env python
# coding: utf-8

# In[1]:


# !pip install -U pandas sqlalchemy psycopg2-binary


# In[2]:


import pandas as pd
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, JSON, Boolean, BigInteger, DateTime, Date, Numeric

# read from the config
try:
    with open("config.json", "r") as f:
        config = json.load(f)

    pg_username = config["pg_username"]
    pg_password = config["pg_password"]

except:
    exit()

engine = create_engine(
    'sqlite:///current_orders.db', 
    echo=False
)

sierra_engine = create_engine(
    'postgresql://{}:{}@sierra-db.plch.net:1032/iii'.format(
        pg_username, pg_password
    )
)

# remove the previous database
try:
    os.remove('current_orders.db')
except:
    pass

os.close(os.open('current_orders.db', os.O_CREAT))


# In[3]:


# %%time
# Fund data

sql = """SELECT 
fm.id AS fund_master_id,
au.code_num AS accounting_unit_code_num,
fm.code_num AS fund_code_num,
fm.code AS fund_code,
fmu.fund_type, -- fbal The linked data is for a current fund.
               -- oldfbal The linked data is for a fund from last year.
               -- fprevbal The linked data is for a Fund Activity Report.
fmu.fund_code,
fmu.external_fund_code_num,
efpn."name" AS external_fund_name, 
fmu.appropriation,
fmu.expenditure,
fmu.encumbrance,
fmu.num_orders,
fmu.num_payments,
fmu.warning_percent,
fmu.discount_percent,
fmu."name",
fmu.note1,
fmu.note2
FROM
sierra_view.fund_master AS fm 
JOIN sierra_view.accounting_unit AS au ON au.id = fm.accounting_unit_id
JOIN sierra_view.fund_myuser AS fmu ON fmu.fund_master_id = fm.id
JOIN sierra_view.external_fund_property AS efp ON efp.code_num = fmu.external_fund_code_num
JOIN sierra_view.external_fund_property_name AS efpn ON efpn.external_fund_property_id = efp.id 
-- sierra_view.fund_myuser AS f
ORDER BY
fm.id
"""

df = pd.read_sql(sql=sql, con=sierra_engine)

df.to_sql(
    name='fund', 
    index=False, 
    if_exists='replace', 
    con=engine, 
    chunksize=10000,
    dtype={
        'fund_master_id': BigInteger(),
        'accounting_unit_code_num': Integer(),
        'fund_code_num': Integer(),
        'fund_code': Text(),
        'fund_type': Text(),
        'fund_code': Text(),
        'external_fund_code_num': Integer(),
        'external_fund_name': Text(),
        'appropriation': Integer(),
        'expenditure': Integer(),
        'encumbrance': Integer(),
        'num_orders': Integer(),
        'num_payments': Integer(),
        'warning_percent': Integer(),
        'discount_percent': Numeric(),
        'name': Text(),
        'note1': Text(),
        'note2': Text()
    }
)


# In[4]:


# %%time

# TODO: consider adding each line for the invoice here... 
sql = """WITH invoice_data AS (
    SELECT
    ir.record_id AS invoice_record_id,
    count(il.id) as count_invoice_record_line,
    json_agg(
        json_build_object( 
            --id
            --invoice_record_id
            'line_cnt', il.line_cnt,
            'order_record_id', il.order_record_metadata_id,
            'order_record_num', rm.record_num,
            'title', il.title,
            'paid_amt_cents', (il.paid_amt * 100.0) :: INTEGER,
            'lien_amt_cents', (il.lien_amt * 100.0) :: INTEGER,
            'lien_flag', il.lien_flag,
            'list_price_cents', (il.list_price * 100.0) :: INTEGER,
            'fund_code', il.fund_code,
            'subfund_num', il.subfund_num,
            'copies_paid_cnt', il.copies_paid_cnt,
            'external_fund_code_num', il.external_fund_code_num,
            'external_fund_code_num', il.status_code,
            'note', il.note,
            'is_single_copy_partial_pmt', il.is_single_copy_partial_pmt,
            'multiflag_code', il.multiflag_code,
            'line_level_tax_cents', (il.line_level_tax * 100.0) :: INTEGER,	
            'vendor_code', il.vendor_code,
            'accounting_transaction_voucher_num', il.accounting_transaction_voucher_num,
            'accounting_transaction_voucher_seq_num', il.accounting_transaction_voucher_seq_num,
            'invoice_record_vendor_summary_id', il.invoice_record_vendor_summary_id,
            'is_use_tax', il.is_use_tax
        )
        ORDER BY
        il.invoice_record_id ASC,
        il.line_cnt ASC,
        il.order_record_metadata_id ASC
    ) AS json_invoice_record_line
    FROM 
    sierra_view.invoice_record AS ir 
    JOIN sierra_view.invoice_record_line AS il ON il.invoice_record_id = ir.record_id 
    JOIN sierra_view.record_metadata AS rm ON rm.id = il.order_record_metadata_id 
    --JOIN sierra_view.invoice_record_vendor_summary AS irvs ON irvs.invoice_record_id = il.invoice_record_id
    GROUP BY 1
)
SELECT
d.invoice_record_id,
rm2.record_num AS invoice_record_num,
d.count_invoice_record_line,
d.json_invoice_record_line,
date(rm2.creation_date_gmt) AS creation_date,
date(rm2.record_last_updated_gmt) AS last_updated,
ir2.accounting_unit_code_num,
date(ir2.invoice_date_gmt) AS invoice_date,
date(ir2.paid_date_gmt) AS paid_date,
ir2.status_code AS invoice_record_status_code,
date(ir2.posted_data_gmt) AS posted_date,
ir2.is_paid_date_received_date,
-- ncode1,
-- ncode2,
-- ncode3,
ir2.invoice_number_text,
ir2.iii_user_name,
ir2.foreign_currency_code,
ir2.foreign_currency_format,
ir2.foreign_currency_exchange_rate,
ir2.tax_fund_code,
ir2.tax_type_code,
(ir2.discount_amt * 100.0) :: INTEGER AS discount_amt_cents,
(ir2.grand_total_amt * 100.0) :: INTEGER AS grand_total_amt_cents,
(ir2.subtotal_amt * 100.0) :: INTEGER AS subtotal_amt_cents,
(ir2.shipping_amt * 100.0) :: INTEGER AS shipping_amt_cents,
(ir2.total_tax_amt * 100.0) :: INTEGER AS total_tax_amt_cents,
ir2.use_tax_fund_code,
ir2.use_tax_percentage_rate,
ir2.use_tax_type_code,
ir2.use_tax_ship_service_code,
ir2.is_suppressed
FROM
invoice_data AS d
JOIN sierra_view.invoice_record AS ir2 ON ir2.record_id = d.invoice_record_id
JOIN sierra_view.record_metadata rm2 ON rm2.id = d.invoice_record_id
"""

df = pd.read_sql(sql=sql, con=sierra_engine)


# In[5]:


df.to_sql(
    name='invoice', 
    index=False, 
    if_exists='replace', 
    con=engine, 
    chunksize=10000,
    dtype={
        'invoice_record_id': BigInteger(),
        'invoice_record_num': Integer(),
        'count_invoice_record_line': Integer(),
        'json_invoice_record_line': JSON(),
        'creation_date': Date(),
        'last_updated': Date(),
        'accounting_unit_code_num': Integer(),
        'invoice_date': Date(),
        'paid_date': Date(),
        'invoice_record_status_code': Text(),
        'posted_date': Date(),
        'is_paid_date_received_date': Boolean(),
        'invoice_number_text': Text(), 
        'iii_user_name': Text(),
        'foreign_currency_code': Text(),
        'foreign_currency_format': Text(), 
        'foreign_currency_exchange_rate': Numeric(),
        'tax_fund_code': Text(),
        'tax_type_code': Text(),
        'discount_amt_cents': Integer(),
        'grand_total_amt_cents': Integer(),
        'subtotal_amt_cents': Integer(),
        'shipping_amt_cents': Integer(),
        'total_tax_amt_cents': Integer(),
        'use_tax_fund_code': Text(),
        'use_tax_percentage_rate': Numeric(),
        'use_tax_type_code': Text(),
        'use_tax_ship_service_code': Text(),
        'is_suppressed': Boolean()
    }
)


# In[6]:


# %%time
# create a linking table between the invoice and order record data ...
sql = """
-- invoice_record_order_record_link
SELECT
DISTINCT
irl.invoice_record_id,
irm.record_num AS invoice_record_num,
irl.order_record_metadata_id AS order_record_id,
orm.record_num AS order_record_num
FROM 
sierra_view.invoice_record_line AS irl
JOIN sierra_view.record_metadata AS irm ON irm.id = irl.invoice_record_id 
JOIN sierra_view.record_metadata AS orm ON orm.id = irl.order_record_metadata_id
ORDER BY
irl.invoice_record_id ASC,
irl.order_record_metadata_id ASC
;
"""

df = pd.read_sql(sql=sql, con=sierra_engine)

df.to_sql(
    name='invoice_record_order_record_link', 
    index=False, 
    if_exists='replace', 
    con=engine, 
    chunksize=10000,
    dtype={
        'invoice_record_id': BigInteger(),
        'invoice_record_num': Integer(),
        'order_record_id': BigInteger(),
        'order_record_num': Integer()
    }
)


# In[7]:


# %%time
sql = """-- build the aggreate cmf data associated with the order record
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
vr.record_id AS vendor_record_id,
order_record.volume_count
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
"""

df = pd.read_sql(sql=sql, con=sierra_engine)


# In[8]:


df.to_sql(
    name='orders', 
    index=False, 
    if_exists='replace', 
    con=engine, 
    chunksize=10000,
    dtype={
        'order_record_num': Integer(), 
        'order_record_id': BigInteger(), 
        'count_cmf': Integer(),
        'count_distinct_cmf_fund_codes': Integer(), 
        'sum_cmf_copies': Integer(), 
        'cmf_data': JSON(),
        'orders_display_order': Integer(), 
        'bib_record_id': BigInteger(), 
        'creation_date_gmt': DateTime(),
        'accounting_unit_code_num': Integer(),
        'estimated_price_cents': Integer(),
        'physical_form_code': Text(),
        'physical_form_name': Text(),
        'order_date_gmt': DateTime(),
        'catalog_date_gmt': DateTime(),
        'order_type_code': Text(),
        'order_type_name': Text(),
        'received_date_gmt': DateTime(),
        'receiving_location_code': Text(),
        'order_status_code': Text(),
        'order_status_name': Text(), 
        'vendor_record_code': Text(), 
        'vendor_record_id': BigInteger(),
        'volume_count': Integer(),        
    }
)


# In[9]:


sql = """WITH vendor_data AS (
    SELECT
    vr.record_id AS vendor_record_id,
    count(vra.id) AS count_address,
    json_agg(
        json_build_object(
          'address_type_code', vrat.code, 
            'display_order', vra.display_order,
            'addr1', vra.addr1,
            'addr2', vra.addr2,
            'addr3', vra.addr3,
            'village', vra.village,
            'city', vra.city,
            'region', vra.region,
            'postal_code', vra.postal_code,
            'country', vra.country,
            'vendor_record_address_id', vra.id 
        )
        ORDER BY
        vrat.code,
        vra.display_order ASC,
        vra.id 
    ) AS vendor_record_address 
    FROM 
    sierra_view.vendor_record AS vr
    JOIN sierra_view.vendor_record_address AS vra ON vra.vendor_record_id = vr.record_id 
    JOIN sierra_view.vendor_record_address_type AS vrat ON vrat.id = vra.vendor_record_address_type_id 
    GROUP BY 1
)
SELECT
d.vendor_record_id,
rm.record_num AS vendor_record_num,
date(rm.creation_date_gmt) AS record_create_date,
date(rm.record_last_updated_gmt) AS record_last_updated,
rm.num_revisions, 
(
    -- the first addr1 value from the first vendor address
    SELECT
    vra2.addr1 
    FROM 
    sierra_view.vendor_record_address AS vra2
    JOIN sierra_view.vendor_record_address_type vrat2 ON vrat2.id = vra2.vendor_record_address_type_id
    WHERE 
    vra2.vendor_record_id = d.vendor_record_id
    ORDER BY
    vrat2.code ASC,
    vra2.display_order ASC
    LIMIT 1
) AS vendor_name,
vr2.code AS vendor_record_code,
vr2.accounting_unit_code_num,
d.count_address,
d.vendor_record_address
FROM
vendor_data AS d
JOIN sierra_view.vendor_record vr2 ON vr2.record_id = d.vendor_record_id
JOIN sierra_view.record_metadata AS rm ON rm.id = vr2.record_id 
ORDER BY 
vr2.record_id
"""

df = pd.read_sql(sql=sql, con=sierra_engine)


# In[10]:


df.to_sql(
    name='vendors', 
    index=False, 
    if_exists='replace', 
    con=engine, 
    chunksize=10000,
    dtype={
        'vendor_record_id': BigInteger(),
        'vendor_record_num': Integer(),
        'record_create_date': Date(),
        'record_last_updated': Date(),
        'num_revisions': Integer(),
        'vendor_name': Text(),
        'vendor_record_code': Text(),
        'accounting_unit_code_num': Integer(),
        'count_address': Integer(),
        'vendor_record_address': JSON() 
    }
)


# In[11]:


# sql = """\
# -- search for a barcode stored in the 'b' tagged varfield for the item...
# SELECT
# items.item_record_id,
# items.item_record_num,
# json_extract(value, '$.field_content') as barcode
# FROM
# items, 
# json_each(items.json_item_varfields)
# WHERE 
# json_extract(value, '$.varfield_type_code') = 'b'
# and json_extract(value, '$.field_content') like '{}'
# """

# pd.read_sql(sql=sql.format('a000073209167'), con=engine).head()


# In[12]:


# !tar -cvvf - ./current_orders.db | xz -9 -T0 > current_orders.db.tar.xz

