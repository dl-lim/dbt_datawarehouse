WITH t AS (
  SELECT
    matomo_log_link_visit_action.idlink_va::BIGINT,
    matomo_log_link_visit_action.server_time::TIMESTAMP AS server_time,
    matomo_log_visit.visit_last_action_time::TIMESTAMP AS visit_last_action_time,
    matomo_log_visit.visit_first_action_time::TIMESTAMP AS visit_first_action_time,
    TO_TIMESTAMP((matomo_log_visit.visitor_localtime::TEXT)::BIGINT)::TIME AS visitor_localtime
  FROM matomo.matomo_log_link_visit_action AS matomo_log_link_visit_action
  LEFT JOIN matomo.matomo_log_visit AS matomo_log_visit ON matomo_log_link_visit_action.idvisit = matomo_log_visit.idvisit
)

SELECT
  matomo_log_link_visit_action.idlink_va::BIGINT,
  matomo_log_visit.idvisit::BIGINT,
  ENCODE(DECODE(matomo_log_visit.idvisitor,'base64'),'hex')::TEXT AS idvisitor, -- HEX type in mysql
  matomo_log_action.idaction::INT,
  matomo_log_visit.idsite::INT,
  matomo_log_action.name::TEXT,
  matomo_log_action.hash::BIGINT, -- INT out of range, use BIGINT
  matomo_log_action.type::SMALLINT,
  matomo_log_action.url_prefix::SMALLINT,
  matomo_log_link_visit_action.idpageview::TEXT,
  t.server_time,
  t.visit_last_action_time,
  t.visit_first_action_time,
  matomo_log_link_visit_action.time_spent::INT,
  matomo_log_link_visit_action.time_server::INT,
  matomo_log_link_visit_action.time_network::INT,
  matomo_log_link_visit_action.time_on_load::INT,
  matomo_log_link_visit_action.time_transfer::INT,
  matomo_log_link_visit_action.time_dom_completion::INT,
  matomo_log_link_visit_action.time_dom_processing::INT,
  matomo_log_link_visit_action.time_spent_ref_action::INT,
  matomo_log_link_visit_action.pageview_position::INT,
  matomo_log_link_visit_action.custom_float::FLOAT,
  matomo_log_link_visit_action.search_cat::TEXT,
  matomo_log_link_visit_action.search_count::INT,
  matomo_log_link_visit_action.product_price::FLOAT,
  matomo_log_link_visit_action.idaction_name::INT AS idaction_name_log_link,
  matomo_log_link_visit_action.idaction_url_ref::INT,
  matomo_log_link_visit_action.idaction_name_ref::INT,
  matomo_log_link_visit_action.idaction_product_cat::INT,
  matomo_log_link_visit_action.idaction_product_sku::INT,
  matomo_log_link_visit_action.idaction_content_name::INT,
  matomo_log_link_visit_action.idaction_event_action::INT,
  matomo_log_link_visit_action.idaction_product_cat2::INT,
  matomo_log_link_visit_action.idaction_product_cat3::INT,
  matomo_log_link_visit_action.idaction_product_cat4::INT,
  matomo_log_link_visit_action.idaction_product_cat5::INT,
  matomo_log_link_visit_action.idaction_product_name::INT,
  matomo_log_link_visit_action.idaction_content_piece::INT,
  matomo_log_link_visit_action.idaction_content_target::INT,
  matomo_log_link_visit_action.idaction_event_category::INT,
  matomo_log_link_visit_action.idaction_content_interaction::INT,
  matomo_log_visit.user_id::TEXT,
  matomo_log_visit.profilable::BOOLEAN,
  COALESCE(matomo_log_visit.campaign_source::TEXT,'untracked') AS mtm_source,
  COALESCE(matomo_log_visit.campaign_medium::TEXT,'untracked') AS mtm_medium,
  COALESCE(matomo_log_visit.campaign_name::TEXT,'untracked') AS mtm_campaign,
  COALESCE(matomo_log_visit.campaign_content::TEXT,'untracked') AS mtm_content,
  COALESCE(matomo_log_visit.campaign_keyword::TEXT,'untracked') AS mtm_kwd,
  COALESCE(matomo_log_visit.campaign_id::TEXT,'untracked') AS mtm_cid,
  COALESCE(matomo_log_visit.campaign_group::TEXT,'untracked') AS mtm_group,
  COALESCE(matomo_log_visit.campaign_placement::TEXT,'untracked') AS mtm_placement,
  matomo_log_visit.referer_url::TEXT,
  matomo_log_visit.referer_name::TEXT,
  matomo_log_visit.referer_type::SMALLINT,
  matomo_log_visit.referer_keyword::TEXT,
  '0.0.0.0'::inet + ('x' || lpad(ENCODE(DECODE(matomo_log_visit.location_ip,'base64'),'hex'), 8, '0'))::bit(32)::bigint AS location_ip, -- INET type, i am a smartboi
  matomo_log_visit.location_city::TEXT,
  matomo_log_visit.location_region::TEXT,
  matomo_log_visit.location_country::TEXT,
  matomo_log_visit.location_latitude::NUMERIC,
  matomo_log_visit.location_longitude::NUMERIC,
  matomo_log_visit.location_browser_lang::TEXT,
  ENCODE(DECODE(matomo_log_visit.config_id,'base64'),'hex')::TEXT AS config_id, -- HEX type in mysql
  matomo_log_visit.config_os::TEXT,
  matomo_log_visit.config_pdf::BOOLEAN,
  matomo_log_visit.config_device_type::SMALLINT,
  matomo_log_visit.config_device_brand::TEXT,
  matomo_log_visit.config_device_model::TEXT,
  matomo_log_visit.config_os_version::TEXT,
  matomo_log_visit.config_client_type::BOOLEAN,
  matomo_log_visit.config_browser_name::TEXT,
  matomo_log_visit.config_browser_version::TEXT,
  matomo_log_visit.config_browser_engine::TEXT,
  matomo_log_visit.config_resolution::TEXT,
  matomo_log_visit.config_cookie::BOOLEAN,
  matomo_log_visit.config_java::BOOLEAN,
  matomo_log_visit.config_flash::BOOLEAN,
  matomo_log_visit.config_quicktime::BOOLEAN,
  matomo_log_visit.config_windowsmedia::BOOLEAN,
  matomo_log_visit.config_realplayer::BOOLEAN,
  matomo_log_visit.config_silverlight::BOOLEAN,
  t.visitor_localtime,
  matomo_log_visit.visitor_returning::BOOLEAN,
  matomo_log_visit.visitor_count_visits::INT,
  matomo_log_visit.visitor_seconds_since_last::INT,
  matomo_log_visit.visitor_seconds_since_first::INT,
  matomo_log_visit.visitor_seconds_since_order::INT,
  matomo_log_visit.last_idlink_va::BIGINT,
  matomo_log_visit.visit_goal_converted::BOOLEAN,
  matomo_log_visit.visit_goal_buyer::BOOLEAN,
  matomo_log_visit.visit_total_time::INT,
  matomo_log_visit.visit_total_events::INT,
  matomo_log_visit.visit_total_actions::INT,
  matomo_log_visit.visit_total_searches::SMALLINT,
  matomo_log_visit.visit_exit_idaction_url::INT,
  matomo_log_visit.visit_entry_idaction_url::INT,
  matomo_log_visit.visit_exit_idaction_name::INT,
  matomo_log_visit.visit_entry_idaction_name::INT,
  matomo_log_visit.visit_total_interactions::INT,
  matomo_log_visit.custom_dimension_1::TEXT,
  matomo_log_visit.custom_dimension_2::TEXT,
  matomo_log_visit.custom_dimension_3::TEXT,
  matomo_log_visit.custom_dimension_4::TEXT,
  matomo_log_visit.custom_dimension_5::TEXT,
  -- hour of week
  CONCAT(
    COALESCE(NULLIF(
    TO_CHAR(t.visit_last_action_time,'D')::INT - 1,0),7)
    ,'_',TO_CHAR(t.visit_last_action_time,'Dy')) AS week_day, --p_dates
  DATE_PART('hour',t.visit_last_action_time)::INT AS week_hour, --p_dates
  -- iso US states
  CONCAT(
		UPPER(matomo_log_visit.location_country::TEXT),
		'-',
		UPPER(matomo_log_visit.location_region::TEXT)
	) AS iso_geo_state,
  -- page views
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
FROM matomo.matomo_log_link_visit_action AS matomo_log_link_visit_action
LEFT JOIN matomo.matomo_log_visit AS matomo_log_visit ON matomo_log_link_visit_action.idvisit = matomo_log_visit.idvisit
LEFT JOIN matomo.matomo_log_action AS matomo_log_action ON matomo_log_link_visit_action.idaction_url = matomo_log_action.idaction
LEFT JOIN t ON matomo_log_link_visit_action.idlink_va = t.idlink_va,

LATERAL
	(SELECT
		NULLIF(SPLIT_PART(matomo_log_action.name, '/', 2),'') AS slug_1,
		NULLIF(SPLIT_PART(matomo_log_action.name, '/', 3),'') AS slug_2,
		NULLIF(SPLIT_PART(matomo_log_action.name, '/', 4),'') AS slug_3,
		NULLIF(SPLIT_PART(matomo_log_action.name, '/', 5),'') AS slug_4,
		NULLIF(SPLIT_PART(matomo_log_action.name, '/', 6),'') AS slug_5
	) s

WHERE matomo_log_visit.idsite = '1' 
AND location_country NOT IN ('xx','xx') --remove traffic from countries
