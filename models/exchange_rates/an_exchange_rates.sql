WITH u AS ( --Denominate with USD
	SELECT
		(_airbyte_data::jsonb->>'date')::DATE AS curr_date,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'USD')::NUMERIC AS usd,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'HKD')::NUMERIC AS hkd,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'GBP')::NUMERIC AS gbp,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'MYR')::NUMERIC AS myr,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'AUD')::NUMERIC AS aud,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'CAD')::NUMERIC AS cad,
		((_airbyte_data::jsonb->>'rates')::jsonb->>'EUR')::NUMERIC AS eur

	FROM exchange_rates._airbyte_raw_exchange_rates
)

SELECT
	curr_date,
	'1'::NUMERIC AS usd,
	ROUND(AVG(hkd / usd) , 5) AS hkd,
	ROUND(AVG(gbp / usd) , 5) AS gbp,
	ROUND(AVG(myr / usd) , 5) AS myr,
	ROUND(AVG(aud / usd) , 5) AS aud,
	ROUND(AVG(cad / usd) , 5) AS cad,
	ROUND(AVG(eur / usd) , 5) AS eur

FROM u
GROUP BY curr_date
