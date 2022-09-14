SELECT
  a.*,
  COALESCE('/' || s.slug_1, '/') ||
		COALESCE('/' || s.slug_2, '') ||
		COALESCE('/' || s.slug_3, '') ||
		COALESCE('/' || s.slug_4, '') ||
		COALESCE('/' || s.slug_5, '') AS page_viewed,
  s.slug_1,
  s.slug_2,
  s.slug_3,
  s.slug_4,
  s.slug_5
FROM an_matomo_complete AS a,
LATERAL
	(SELECT
		NULLIF(SPLIT_PART(a.name, '/', 2),'') AS slug_1,
		NULLIF(SPLIT_PART(a.name, '/', 3),'') AS slug_2,
		NULLIF(SPLIT_PART(a.name, '/', 4),'') AS slug_3,
		NULLIF(SPLIT_PART(a.name, '/', 5),'') AS slug_4,
		NULLIF(SPLIT_PART(a.name, '/', 6),'') AS slug_5
	) s
WHERE a.type = 1 -- page views only
