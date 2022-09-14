WITH

date_ticks AS (
	SELECT
		generate_series('2021-01-01'::DATE, current_date, interval '1 day')::date AS date_tick
),


daily_facebook AS (
	SELECT
	fb.date_start AS fb_date,
	ROUND((fb.spend/exc.gbp)::NUMERIC, 2) AS fb_amount_usd
	FROM (
		SELECT
		date_start,
		SUM(spend) AS spend
		FROM an_facebook_ads
		GROUP BY date_start
	) fb
	LEFT JOIN LATERAL (
		SELECT
			exc.curr_date,
			exc.gbp
		FROM an_exchange_rates exc
		WHERE exc.curr_date <= fb.date_start
		ORDER BY exc.curr_date DESC
		LIMIT 1
	) exc ON 1=1

),

daily_fulfillrite AS (
	SELECT
		date::date AS fu_date, --rename this!
		ABS(ROUND(SUM(amount::NUMERIC),2)) AS fu_amount
	FROM fulfillrite.fulfillrite
	WHERE category IN (
		'Order Fulfillment',
		'Shipping',
		'Return Processing',
		'Supplies')
	GROUP BY date::DATE
),

daily_orders AS (
	SELECT
		date_created::DATE AS or_date,
		ROUND(SUM(total_sales),2) AS or_amount
	FROM {{ref('an_orders_complete')}}
	GROUP BY date_created::DATE
),

daily_stripe AS (
	SELECT
		date_created::DATE AS st_date,
		ROUND(SUM(fee_src::NUMERIC),2) AS st_amount
	FROM {{ref('an_stripe')}}
	GROUP BY date_created::DATE
)


SELECT
	d.date_tick,
	EXTRACT(isodow FROM d.date_tick::DATE) AS week_day,
	fb.fb_amount_usd AS "3_facebook_costs",
	fu.fu_amount AS "2_fulfillment_costs",
	s.st_amount AS "1_stripe_fee",
	o.or_amount AS "0_total_sales",
	COALESCE(o.or_amount,0) - COALESCE(fb.fb_amount_usd,0) - COALESCE(fu.fu_amount,0) - COALESCE(s.st_amount,0) AS "9_profit"
FROM date_ticks d
LEFT JOIN daily_facebook fb ON d.date_tick = fb.fb_date
LEFT JOIN daily_fulfillrite fu ON d.date_tick = fu.fu_date --rename this!
LEFT JOIN daily_orders o ON d.date_tick = o.or_date
LEFT JOIN daily_stripe s ON d.date_tick = s.st_date
ORDER BY d.date_tick
