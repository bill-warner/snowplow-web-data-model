DROP TABLE IF EXISTS web.page_performance;
CREATE TABLE web.page_performance
  DISTKEY(page_view_id)
  SORTKEY(page_view_id)
AS (
  WITH user_sessions AS (
  	SELECT
  	  page_views.user_snowplow_domain_id,
  	  page_views.page_view_id,
  	  COUNT(DISTINCT page_views.session_id) sessions

  	FROM web.page_views_tmp AS page_views
  	GROUP BY 1,2
  	)

  , total_sessions AS (
  	SELECT 
  	  sessions.page_view_id,
  	  SUM(sessions.sessions) AS sessions

  	FROM user_sessions
  	GROUP BY 1)

  , page_views AS (
  SELECT
  	page_views.page_url,
  	page_views.page_title,
  	page_views.sessions,
  	MIN(page_views.min_tstamp)::date first_viewed,
  	SUM(page_views.total_time_in_ms) total_loading_time_ms,
  	SUM(page_views.time_engaged_in_s) AS time_engaged_in_s,
  	SUM(page_views.horizontal_percentage_scrolled) AS horizontal_percentage_scrolled,
  	SUM(page_views.vertical_percentage_scrolled) AS vertical_percentage_scrolled,
  	COUNT(DISTINCT page_views.page_view_id) page_views,
  	COUNT(DISTINCT CASE WHEN page_views.dvce_ismobile THEN page_views.page_view_id END) mobile_page_views,
  	COUNT(DISTINCT CASE WHEN page_views.min_tstamp >= DATEADD(d, -7, GETDATE()) AND page_views.min_tstamp < GETDATE() THEN page_views.page_view_id END) page_views_current_7d,
  	COUNT(DISTINCT CASE WHEN page_views.min_tstamp >= DATEADD(d, -14, GETDATE()) AND page_views.min_tstamp < DATEADD(d, -7, GETDATE()) THEN page_views.page_view_id END) page_views_previous_7d,
  	COUNT(DISTINCT CASE WHEN page_views.min_tstamp >= DATEADD(d, -30, GETDATE()) AND page_views.min_tstamp < GETDATE() THEN page_views.page_view_id END) page_views_current_30d,
  	COUNT(DISTINCT CASE WHEN page_views.min_tstamp >= DATEADD(d, -60, GETDATE()) AND page_views.min_tstamp < DATEADD(d, -30, GETDATE()) THEN page_views.page_view_id END) page_views_previous_30d,
  	COUNT(DISTINCT CASE WHEN page_views.min_tstamp >= DATEADD(month, -6, GETDATE()) AND page_views.min_tstamp < GETDATE() THEN page_views.page_view_id END) page_views_current_6m,
  	COUNT(DISTINCT CASE WHEN page_views.min_tstamp >= DATEADD(month, -12, GETDATE()) AND page_views.min_tstamp < DATEADD(month, -6, GETDATE()) THEN page_views.page_view_id END) page_views_previous_6m,
  	COUNT(DISTINCT page_views.stitched_user_id) users,
  	COUNT(DISTINCT CASE WHEN page_views.user_bounced THEN page_views.stiched_user_id END) users_bounced,
  	COUNT(DISTINCT CASE WHEN page_views.user_engaged THEN page_views.stiched_user_id END) users_engaged,
  	COUNT(DISTINCT CASE WHEN page_views.page_view_index = 1 THEN page_views.page_view_id ELSE NULL END) first_page_views,
  	COUNT(DISTINCT CASE WHEN page_views.page_view_in_session_index = 1 THEN page_views.page_view_id ELSE NULL END) first_page_in_session_views,
  	COUNT(DISTINCT CASE WHEN page_views.page_view_in_session_reverse_index = 1 THEN page_views.page_view_id ELSE NULL END) last_page_in_session_views,
  	SUM(page_views.page_view_in_session_index) page_view_in_session_index

  FROM web.page_views_tmp AS page_views
  INNER JOIN total_sessions 
  	ON page_views.page_view_id = total_sessions.page_view_id

  GROUP BY 1,2,3)

  SELECT
    *
    DATEDIFF(d, pv.first_viewed, CURRENT_DATE()) days_since_first_view
  FROM page_views AS pv
  );