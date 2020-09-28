--AVG page view depth per session.

--Nice to haves: Cost to cal ROI. Conversion rates

DROP TABLE IF EXISTS web.marketing_campaigns_tmp;
CREATE TABLE web.marketing_campaigns_tmp
  DISTKEY(marketing_campaign)
  SORTKEY(marketing_campaign)
AS (
	WITH campaigns AS (
	SELECT
		sessions.marketing_medium,
	    sessions.marketing_source,
	    sessions.marketing_term,
	    sessions.marketing_content,
	    sessions.marketing_campaign,
	    sessions.marketing_click_id,
	    sessions.marketing_network,
	    MIN(sessions.session_start) campaign_start,
	    COUNT(DISTINCT sessions.stitched_user_id) AS users,
	    COUNT(DISTINCT CASE WHEN sessions.user_bounced THEN page_views.stiched_user_id END) AS users_bounced,
	  	COUNT(DISTINCT CASE WHEN sessions.user_engaged THEN page_views.stiched_user_id END) AS users_engaged,
	    COUNT(DISTINCT CASE WHEN sessions.first_user_session THEN sessions.stitched_user_id END) new_users,
	    COUNT(DISTINCT CASE WHEN DATEDIFF(d, sessions.previous_session_start, sessions.session_start) > 30 
	    					THEN sessions.stitched_user_id END) AS reactivated_users, --added. Assumed if gone for more than 30 days then churned.
	    COUNT(DISTINCT CASE WHEN DATEDIFF(d, sessions.session_start, sessions.next_session_start) <= 30 
	    					THEN sessions.stitched_user_id END) AS repeat_users_within_30d, --added. Not accurate for data within last 30 days. 
	    COUNT(DISTINCT sessions.session_id) AS sessions,
	    SUM(sessions.page_views) AS page_views,
	    SUM(sessions.time_engaged_in_s) AS time_engaged_in_s,
	    SUM(DATEDIFF(s, sessions.session_start, sessions.session_end)) sessions_length_in_s


	FROM web.sessions_tmp AS sessions
	WHERE sessions.marketing_campaign IS NOT NULL

	GROUP BY 1,2,3,4,5,6,7
	)

	SELECT
		*,
		DATEDIFF(d, s.campaing_start, CURRENT_DATE()) AS campaign_days_active
	FROM campaigns AS c
	);
