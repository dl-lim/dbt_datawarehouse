SELECT
  DISTINCT m.idvisit,
  m.visit_last_action_time AS visit_time,
  CASE WHEN c.order_no IS NOT NULL THEN '1' ELSE '0' END AS purchaser
FROM {{ref('an_matomo_complete')}} AS m
LEFT JOIN {{ref('an_matomo_conversion')}} AS c ON m.idvisit = c.idvisit
