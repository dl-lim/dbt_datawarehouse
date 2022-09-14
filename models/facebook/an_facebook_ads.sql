WITH ab AS (
   SELECT
      ab.f_id || '_' || ab.ab_date AS id
   FROM (
      SELECT
      (_airbyte_data::jsonb->>'account_id') || '_' || (_airbyte_data::jsonb->>'campaign_id') || '_' || (_airbyte_data::jsonb->>'adset_id') || '_' || (_airbyte_data::jsonb->>'ad_id') || '_' || (_airbyte_data::jsonb->>'date_start') AS f_id,
      MAX(_airbyte_emitted_at) AS ab_date
      FROM facebook._airbyte_raw_ads_insights
      GROUP BY (_airbyte_data::jsonb->>'account_id') || '_' || (_airbyte_data::jsonb->>'campaign_id') || '_' || (_airbyte_data::jsonb->>'adset_id') || '_' || (_airbyte_data::jsonb->>'ad_id') || '_' || (_airbyte_data::jsonb->>'date_start')
   ) ab
),

f AS (
  -- basic ads_insights data
  SELECT
    _airbyte_data::jsonb->>'ad_id' AS ad_id,
    _airbyte_data::jsonb->>'adset_id' AS adset_id,
    _airbyte_data::jsonb->>'campaign_id' AS campaign_id,
    _airbyte_data::jsonb->>'account_id' AS account_id,
    _airbyte_data::jsonb->>'clicks' AS clicks,
    _airbyte_data::jsonb->>'cpc' AS cpc,
    _airbyte_data::jsonb->>'ctr' AS ctr,
    _airbyte_data::jsonb->>'cpm' AS cpm,
    _airbyte_data::jsonb->>'cpp' AS cpp,
    _airbyte_data::jsonb->>'reach' AS reach,
    _airbyte_data::jsonb->>'spend' AS spend,
    _airbyte_data::jsonb->>'ad_name' AS ad_name,
    _airbyte_data::jsonb->>'date_stop' AS date_stop,
    _airbyte_data::jsonb->>'frequency' AS frequency,
    _airbyte_data::jsonb->>'objective' AS objective,
    _airbyte_data::jsonb->>'adset_name' AS adset_name,
    _airbyte_data::jsonb->>'date_start' AS date_start,
    _airbyte_data::jsonb->>'unique_ctr' AS unique_ctr,
    _airbyte_data::jsonb->>'buying_type' AS buying_type,
    _airbyte_data::jsonb->>'impressions' AS impressions,
    _airbyte_data::jsonb->>'account_name' AS account_name,
    _airbyte_data::jsonb->>'social_spend' AS social_spend,
    _airbyte_data::jsonb->>'age_targeting' AS age_targeting,
    _airbyte_data::jsonb->>'campaign_name' AS campaign_name,
    _airbyte_data::jsonb->>'unique_clicks' AS unique_clicks,
    _airbyte_data::jsonb->>'quality_ranking' AS quality_ranking,
    _airbyte_data::jsonb->>'account_currency' AS account_currency,
    _airbyte_data::jsonb->>'gender_targeting' AS gender_targeting,
    _airbyte_data::jsonb->>'optimization_goal' AS optimization_goal,
    _airbyte_data::jsonb->>'inline_link_clicks' AS inline_link_clicks,
    _airbyte_data::jsonb->>'cost_per_unique_click' AS cost_per_unique_click,
    _airbyte_data::jsonb->>'full_view_impressions' AS full_view_impressions,
    _airbyte_data::jsonb->>'inline_link_click_ctr' AS inline_link_click_ctr,
    _airbyte_data::jsonb->>'inline_post_engagement' AS inline_post_engagement,
    _airbyte_data::jsonb->>'unique_link_clicks_ctr' AS unique_link_clicks_ctr,
    _airbyte_data::jsonb->>'unique_inline_link_clicks' AS unique_inline_link_clicks,
    _airbyte_data::jsonb->>'cost_per_inline_link_click' AS cost_per_inline_link_click,
    _airbyte_data::jsonb->>'unique_inline_link_click_ctr' AS unique_inline_link_click_ctr,
    _airbyte_data::jsonb->>'cost_per_inline_post_engagement' AS cost_per_inline_post_engagement,
    _airbyte_data::jsonb->>'cost_per_unique_inline_link_click' AS cost_per_unique_inline_link_click,
    (_airbyte_data::jsonb->>'account_id') || '_' || (_airbyte_data::jsonb->>'campaign_id') || '_' || (_airbyte_data::jsonb->>'adset_id') || '_' || (_airbyte_data::jsonb->>'ad_id') || '_' || (_airbyte_data::jsonb->>'date_start') || '_' || _airbyte_emitted_at AS id,

    BTRIM(_airbyte_data::jsonb->>'unique_outbound_clicks','[]')::jsonb->>'value' AS unique_outbound_clicks,
    BTRIM(_airbyte_data::jsonb->>'video_play_curve_actions','[]')::jsonb->>'value' AS video_play_curve_actions
  FROM facebook._airbyte_raw_ads_insights
),

c AS (
   -- conversions/actions info
   SELECT * FROM
      public.crosstab(
         -- actions turned into key-value pair, to be transposed with crosstab
         $ct$
            SELECT
               b.id,
               x->>'action_type' AS action_type,
               x->>'value' AS value
            FROM (
               -- Nested subquery for all actions
               SELECT
                  (_airbyte_data::jsonb->>'account_id') || '_' || (_airbyte_data::jsonb->>'campaign_id') || '_' || (_airbyte_data::jsonb->>'adset_id') || '_' || (_airbyte_data::jsonb->>'ad_id') || '_' || (_airbyte_data::jsonb->>'date_start') || '_' || _airbyte_emitted_at AS id,
                  _airbyte_data::json->'actions' AS actions
               FROM facebook._airbyte_raw_ads_insights
            ) b
            CROSS JOIN jsonb_array_elements(b.actions::jsonb) as x(element)
            ORDER BY 1,2
         $ct$,
         $value$
            VALUES
            ('video_view'),
            ('post_engagement'),
            ('post_reaction'),
            ('like'),
            ('comment'),
            ('post'),
            ('page_engagement'),
            ('link_click'),
            ('landing_page_view'),
            ('view_content'),
            ('lead'),
            ('add_to_cart'),
            ('initiate_checkout'),
            ('add_payment_info'),
            ('purchase'),
            ('omni_view_content'),
            ('omni_add_to_cart'),
            ('omni_initiated_checkout'),
            ('omni_purchase'),
            ('onsite_conversion.messaging_first_reply'),
            ('onsite_conversion.messaging_conversation_started_7d'),
            ('onsite_conversion.post_save'),
            ('offsite_conversion.fb_pixel_view_content'),
            ('offsite_conversion.fb_pixel_lead'),
            ('offsite_conversion.fb_pixel_add_to_cart'),
            ('offsite_conversion.fb_pixel_initiate_checkout'),
            ('offsite_conversion.fb_pixel_add_payment_info'),
            ('offsite_conversion.fb_pixel_purchase')
         $value$
      )
      AS ct(
        id TEXT,
        video_view INT,
        post_engagement INT,
        post_reaction INT,
        likes INT,
        comment INT,
        post INT,
        page_engagement INT,
        link_click INT,
        landing_page_view INT,
        view_content INT,
        leads INT,
        add_to_cart INT,
        initiate_checkout INT,
        add_payment_info INT,
        purchase INT,
        omni_view_content INT,
        omni_add_to_cart INT,
        omni_initiated_checkout INT,
        omni_purchase INT,
        messaging_first_reply INT,
        messaging_conversation_started_7d INT,
        post_save INT,
        fb_pixel_view_content INT,
        fb_pixel_lead INT,
        fb_pixel_add_to_cart INT,
        fb_pixel_initiate_checkout INT,
        fb_pixel_add_payment_info INT,
        fb_pixel_purchase INT
      )
)

SELECT
  ab.id,
  f.ad_id,
  f.adset_id,
  f.campaign_id,
  f.ad_name,
  f.adset_name,
  f.campaign_name,
  f.date_start::DATE,
  f.date_stop::DATE,
  f.objective,
  f.optimization_goal,
  f.spend::NUMERIC,
  f.impressions,
  f.cpm AS cost_per_impression,
  f.reach,
  f.cpp AS cost_per_reach,
  f.frequency,
  f.clicks AS clicks_all, -- all the clicks on all the impressions
  f.ctr AS ctr_clicks_all, -- all the clicks divided by all the impressions
  f.cpc AS cost_per_click_all, -- amount spent divided by all the clicks
  f.unique_clicks, -- number of users who clicked
  f.unique_ctr AS ctr_unique_clicks, -- number of clicked users divided by all the users (reach)
  f.cost_per_unique_click, -- amount spent divided by all the clicked users
  f.unique_outbound_clicks, -- number of clicked users who went outbound
  f.unique_outbound_clicks::NUMERIC/f.reach::NUMERIC AS ctr_unique_outbound_clicks, -- number of clicked users who went outbound per reached users
  f.spend::NUMERIC/f.unique_outbound_clicks::NUMERIC AS cost_per_unique_outbound_click, -- amount spent divided by number of clicked users who went outbound
  f.account_currency,
  f.account_id,
  f.account_name,
  f.buying_type,
  f.inline_post_engagement,
  f.cost_per_inline_post_engagement,
  c.video_view,
  c.post_engagement,
  c.post_reaction,
  c.likes,
  c.comment,
  c.post,
  c.page_engagement,
  c.link_click,
  c.landing_page_view,
  c.view_content,
  c.leads,
  c.add_to_cart,
  c.initiate_checkout,
  c.add_payment_info,
  c.purchase,
  c.omni_view_content,
  c.omni_add_to_cart,
  c.omni_initiated_checkout,
  c.omni_purchase,
  c.messaging_first_reply,
  c.messaging_conversation_started_7d,
  c.post_save,
  c.fb_pixel_view_content,
  c.fb_pixel_lead,
  c.fb_pixel_add_to_cart,
  c.fb_pixel_initiate_checkout,
  c.fb_pixel_add_payment_info,
  c.fb_pixel_purchase
FROM ab
LEFT JOIN f ON ab.id = f.id
LEFT JOIN c on ab.id = c.id
