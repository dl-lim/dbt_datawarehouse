{{
  config({
    "pre-hook": [
      "CREATE EXTENSION IF NOT EXISTS tablefunc SCHEMA public;"
    ]
  })
}}


WITH p_dates AS ( --parsed dates
  SELECT
    o.order_id::INT AS order_id,
    TO_TIMESTAMP(left(o.date_created::TEXT,-3)::INT)::TIMESTAMP WITHOUT TIME ZONE AS date_created,
    TO_TIMESTAMP(left(o.date_created_gmt::TEXT,-3)::INT) AS date_created_gmt
  FROM store.db_wc_order_stats AS o
),

order_meta AS ( --transpose of db_postmeta
  SELECT * FROM
  public.crosstab(
    $ct$
      SELECT
        post_id,
        meta_key,
        meta_value
      FROM store.db_postmeta
      ORDER BY 1,2 -- this line is necessary!
    $ct$,
    $value$
      VALUES
      ('_wcson_order_number'::TEXT),
      ('_order_key'::TEXT),
      ('_payment_method'::TEXT),
      ('_payment_method_title'::TEXT),
      ('_customer_ip_address'::TEXT),
      ('_customer_user_agent'::TEXT),
      ('_cart_hash'::TEXT),
      ('_billing_first_name'::TEXT),
      ('_billing_last_name'::TEXT),
      ('_billing_address_1'::TEXT),
      ('_billing_city'::TEXT),
      ('_billing_state'::TEXT),
      ('_billing_postcode'::TEXT),
      ('_billing_country'::TEXT),
      ('_billing_email'::TEXT),
      ('_shipping_first_name'::TEXT),
      ('_shipping_last_name'::TEXT),
      ('_shipping_address_1'::TEXT),
      ('_shipping_city'::TEXT),
      ('_shipping_state'::TEXT),
      ('_shipping_postcode'::TEXT),
      ('_shipping_country'::TEXT),
      ('_order_currency'::TEXT),
      ('_cart_discount'::TEXT),
      ('_payment_intent_id'::TEXT),
      ('_payment_method_token'::TEXT),
      ('_wc_stripe_mode'::TEXT), --deprecated
      ('_wc_stripe_charge_status'::TEXT),
      ('_transaction_id'::TEXT),
      ('_stripe_source_id'::TEXT),
      ('_stripe_intent_id'::TEXT)
    $value$
  )
  AS ct(
    order_id INT,
    order_no TEXT,
    order_key TEXT,
    payment_method TEXT,
    payment_method_title TEXT,
    customer_ip_address TEXT,
    customer_user_agent TEXT,
    cart_hash TEXT,
    billing_first_name TEXT,
    billing_last_name TEXT,
    billing_address_1 TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_postcode TEXT,
    billing_country TEXT,
    billing_email TEXT,
    shipping_first_name TEXT,
    shipping_last_name TEXT,
    shipping_address_1 TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    shipping_postcode TEXT,
    shipping_country TEXT,
    order_currency TEXT,
    cart_discount TEXT,
    payment_intent_id TEXT,
    payment_method_token TEXT,
    wc_stripe_mode TEXT,
    wc_stripe_charge_status TEXT,
    transaction_id TEXT,
    stripe_source_id TEXT,
    stripe_intent_id TEXT
  )
  WHERE order_no IS NOT NULL
)

SELECT
	DISTINCT o.order_id::INT,
	m.order_no,
	o.status,
	o.net_total,
	o.parent_id::INT,
	o.tax_total,
	o.customer_id::INT,
	o.total_sales,
  p.date_created, --p_dates
	o.num_items_sold::INT,
	o.shipping_total,
  p.date_created_gmt, --p_dates
	o.returning_customer,
	m.order_key,
	m.payment_method,
	m.payment_method_title,
	m.customer_ip_address,
	m.customer_user_agent,
	m.cart_hash,
	m.billing_first_name,
	m.billing_last_name,
	m.billing_address_1,
	m.billing_city,
	m.billing_state,
	m.billing_postcode,
	m.billing_country,
	m.billing_email,
	m.shipping_first_name,
	m.shipping_last_name,
	m.shipping_address_1,
	m.shipping_city,
	m.shipping_state,
	m.shipping_postcode,
	m.shipping_country,
	m.order_currency,
	m.cart_discount,
	COALESCE(m.payment_intent_id,m.stripe_intent_id) AS payment_intent_id,
	COALESCE(m.payment_method_token,m.stripe_source_id) AS payment_method_token, --src_ and pm_ may be different
	m.wc_stripe_mode,
	m.wc_stripe_charge_status,
	m.transaction_id,
  s.status AS stripe_status,
	CONCAT(
		COALESCE(NULLIF(
		TO_CHAR(p.date_created,'D')::INT - 1,0),7)
		,'_',TO_CHAR(p.date_created,'Dy')) AS week_day, --p_dates
	DATE_PART('hour',p.date_created)::INT AS week_hour, --p_dates
	CONCAT(
		COALESCE(shipping_country,billing_country),
		'-',
		COALESCE(shipping_state,billing_state)
	) AS iso_geo_state,
  COALESCE(a.mtm_source,'untracked') AS mtm_source,
  COALESCE(a.mtm_medium,'untracked') AS mtm_medium,
  COALESCE(a.mtm_campaign,'untracked') AS mtm_campaign,
  COALESCE(a.mtm_content,'untracked') AS mtm_content,
  COALESCE(a.mtm_kwd,'untracked') AS mtm_kwd,
  COALESCE(a.mtm_cid,'untracked') AS mtm_cid,
  COALESCE(a.mtm_group,'untracked') AS mtm_group,
  COALESCE(a.mtm_placement,'untracked') AS mtm_placement,
  a.config_os,
  a.config_os_version,
  a.config_browser_name,
  a.visitor_localtime,
  ROUND(a.visitor_seconds_since_last::NUMERIC/86400,2) AS days_since_last
FROM store.db_wc_order_stats AS o
LEFT JOIN p_dates AS p ON o.order_id = p.order_id
LEFT JOIN order_meta AS m ON o.order_id = m.order_id
LEFT JOIN {{ref('an_stripe')}} AS s ON m.transaction_id = s.id --checks if trx is found in live
LEFT JOIN {{ref('an_matomo_conversion')}} AS a ON m.order_no = a.order_no::TEXT
WHERE s.id IS NOT NULL  --filters for orders on LIVE stripe only (test mode orders are excluded)
