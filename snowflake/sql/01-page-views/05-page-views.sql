-- Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     0.1.0
--
-- Authors:     Christophe Bogaert, Colm O Griobhtha
-- Copyright:   Copyright (c) 2018 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE OR REPLACE TABLE web.page_views_tmp
AS (
SELECT

  -- user

  a.user_id AS user_custom_id,
  a.domain_userid AS user_snowplow_domain_id,
  a.network_userid AS user_snowplow_crossdomain_id,

  -- session

  a.domain_sessionid AS session_id,
  a.domain_sessionidx AS session_index,

  -- page view

  a.page_view_id,

  ROW_NUMBER() OVER (PARTITION BY a.domain_userid ORDER BY b.min_tstamp) AS page_view_index,
  ROW_NUMBER() OVER (PARTITION BY a.domain_sessionid ORDER BY b.min_tstamp) AS page_view_in_session_index,

  -- page view: time

  CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp) AS page_view_start,
  CONVERT_TIMEZONE('UTC', 'Europe/London', b.max_tstamp) AS page_view_end,

    -- example derived dimensions

    TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD HH24:MI:SS') AS page_view_time,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD HH24:MI') AS page_view_minute,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD HH24') AS page_view_hour,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM-DD') AS page_view_date,
    TO_CHAR(DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp)), 'YYYY-MM-DD') AS page_view_week,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp), 'YYYY-MM') AS page_view_month,
    TO_CHAR(DATE_TRUNC('quarter', CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp)), 'YYYY-MM') AS page_view_quarter,
    DATE_PART(Y, CONVERT_TIMEZONE('UTC', 'Europe/London', b.min_tstamp))::INTEGER AS page_view_year,


  -- page view time in the user's local timezone. Use OS timezone if available: if not, fall back on geo_timezone, if both are null default to UTC

  CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp) AS page_view_start_local,
  CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp) AS page_view_end_local,

    -- example derived dimensions
    TO_CHAR(CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp), 'YYYY-MM-DD HH24:MI:SS') AS page_view_local_time,
    TO_CHAR(CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp), 'HH24:MI') AS page_view_local_time_of_day,
    DATE_PART(hour, CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp))::INTEGER AS page_view_local_hour_of_day,
    TRIM(TO_CHAR(CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp), 'd')) AS page_view_local_day_of_week,
    DAYOFWEEK(CONVERT_TIMEZONE('UTC', COALESCE(os_timezone, geo_timezone, 'UTC'), b.min_tstamp)) AS page_view_local_day_of_week_index,


  -- engagement

  b.time_engaged_in_s,

  CASE
    WHEN b.time_engaged_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
    WHEN b.time_engaged_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
    WHEN b.time_engaged_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
    WHEN b.time_engaged_in_s > 59 THEN '60s or more'
    ELSE NULL
  END AS time_engaged_in_s_tier,

  c.hmax AS horizontal_pixels_scrolled,
  c.vmax AS vertical_pixels_scrolled,

  c.relative_hmax AS horizontal_percentage_scrolled,
  c.relative_vmax AS vertical_percentage_scrolled,

  CASE
    WHEN c.relative_vmax BETWEEN 0 AND 24 THEN '0% to 24%'
    WHEN c.relative_vmax BETWEEN 25 AND 49 THEN '25% to 49%'
    WHEN c.relative_vmax BETWEEN 50 AND 74 THEN '50% to 74%'
    WHEN c.relative_vmax BETWEEN 75 AND 100 THEN '75% to 100%'
    ELSE NULL
  END AS vertical_percentage_scrolled_tier,

  CASE WHEN b.time_engaged_in_s = 0 THEN TRUE ELSE FALSE END AS user_bounced,
  CASE WHEN b.time_engaged_in_s >= 30 AND c.relative_vmax >= 25 THEN TRUE ELSE FALSE END AS user_engaged,

  -- page

  a.page_urlhost || a.page_urlpath AS page_url,

  a.page_urlscheme AS page_url_scheme,
  a.page_urlhost AS page_url_host,
  a.page_urlport AS page_url_port,
  a.page_urlpath AS page_url_path,
  a.page_urlquery AS page_url_query,
  a.page_urlfragment AS page_url_fragment,

  a.page_title,

  c.doc_width AS page_width,
  c.doc_height AS page_height,

  -- referer

  a.refr_urlhost || a.refr_urlpath AS referer_url,

  a.refr_urlscheme AS referer_url_scheme,
  a.refr_urlhost AS referer_url_host,
  a.refr_urlport AS referer_url_port,
  a.refr_urlpath AS referer_url_path,
  a.refr_urlquery AS referer_url_query,
  a.refr_urlfragment AS referer_url_fragment,

  CASE
    WHEN a.refr_medium IS NULL THEN 'direct'
    WHEN a.refr_medium = 'unknown' THEN 'other'
    ELSE a.refr_medium
  END AS referer_medium,
  a.refr_source AS referer_source,
  a.refr_term AS referer_term,

  -- marketing

  a.mkt_medium AS marketing_medium,
  a.mkt_source AS marketing_source,
  a.mkt_term AS marketing_term,
  a.mkt_content AS marketing_content,
  a.mkt_campaign AS marketing_campaign,
  a.mkt_clickid AS marketing_click_id,
  a.mkt_network AS marketing_network,

  -- location

  a.geo_country,
  a.geo_region,
  a.geo_region_name,
  a.geo_city,
  a.geo_zipcode,
  a.geo_latitude,
  a.geo_longitude,
  a.geo_timezone, -- often NULL (use os_timezone instead)

  -- IP

  a.user_ipaddress AS ip_address,

  a.ip_isp,
  a.ip_organization,
  a.ip_domain,
  a.ip_netspeed AS ip_net_speed,

  -- application

  a.app_id,

  -- browser

  a.useragent_version AS browser,
  a.useragent_family AS browser_name,
  a.useragent_major AS browser_major_version,
  a.useragent_minor AS browser_minor_version,
  a.useragent_patch AS browser_build_version,
  a.br_renderengine AS browser_engine,

  c.br_viewwidth AS browser_window_width,
  c.br_viewheight AS browser_window_height,

  a.br_lang AS browser_language,

  -- OS

  a.os_version AS os,
  a.os_family AS os_name,
  a.os_major AS os_major_version,
  a.os_minor AS os_minor_version,
  a.os_patch AS os_build_version,
  a.os_manufacturer,
  a.os_timezone,

  -- device

  a.device_family AS device,
  a.dvce_type AS device_type,
  a.dvce_ismobile AS device_is_mobile,

  -- page performance

  d.redirect_time_in_ms,
  d.unload_time_in_ms,
  d.app_cache_time_in_ms,
  d.dns_time_in_ms,
  d.tcp_time_in_ms,
  d.request_time_in_ms,
  d.response_time_in_ms,
  d.processing_time_in_ms,
  d.dom_loading_to_interactive_time_in_ms,
  d.dom_interactive_to_complete_time_in_ms,
  d.onload_time_in_ms,
  d.total_time_in_ms

FROM scratch.web_events AS a -- the INNER JOIN requires that all contexts are set

INNER JOIN scratch.web_events_time AS b
  ON a.page_view_id = b.page_view_id

INNER JOIN scratch.web_events_scroll_depth AS c
  ON a.page_view_id = c.page_view_id

INNER JOIN scratch.web_timing_context AS d
  ON a.page_view_id = d.page_view_id

WHERE a.br_family != 'Robot/Spider'
  AND NOT RLIKE(a.useragent, '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%')
  AND a.domain_userid IS NOT NULL -- rare edge case
  AND a.domain_sessionidx > 0 -- rare edge case
  -- AND a.app_id IN ('demo-app')
  -- AND a.page_urlhost IN ('website.com', 'another.websitd.com')
  -- AND a.name_tracker = 'namespace'
);