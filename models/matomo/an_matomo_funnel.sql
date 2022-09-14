WITH
/* -- column method only
date_ticks AS (
	SELECT
		generate_series('2021-01-01'::DATE, current_date, interval '1 day')::date AS date_tick
),
*/
vc AS (
	SELECT
		visit_last_action_time::DATE AS date_tick,
		'view_content' AS type,
		COUNT(DISTINCT idvisit) AS count_users
	FROM {{ref('an_matomo_complete')}}
	GROUP BY visit_last_action_time::DATE
),

ic AS (
	SELECT
		visit_last_action_time::DATE AS date_tick,
		'initiate_checkout' AS type,
		COUNT(DISTINCT idvisit) AS count_users
	FROM {{ref('an_matomo_conversion')}}
	WHERE idgoal = -1
	GROUP BY visit_last_action_time::DATE
),

pur AS (
	SELECT
		visit_last_action_time::DATE AS date_tick,
		'purchase' AS type,
		COUNT(DISTINCT order_no) AS count_users
	FROM {{ref('an_matomo_conversion')}}
	WHERE order_no IS NOT NULL
	GROUP BY visit_last_action_time::DATE
)

SELECT * FROM vc
UNION ALL
SELECT * FROM ic
UNION ALL
SELECT * FROM pur

/*
column method

SELECT
	d.date_tick,
	COALESCE(vc.count_vc,0) AS view_content,
	COALESCE(ic.count_ic,0) AS initiate_checkout,
	COALESCE(pur.count_pur,0) AS purchase
FROM date_ticks d
LEFT JOIN vc ON d.date_tick = vc.date_vc
LEFT JOIN ic ON d.date_tick = ic.date_ic
LEFT JOIN pur ON d.date_tick = pur.date_pur

*/
