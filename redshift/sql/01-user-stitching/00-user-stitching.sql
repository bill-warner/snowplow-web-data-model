-- User stitching model.
-- First example of simple model.
-- Second example of time dependent model

DROP TABLE IF EXISTS web.user_stitching_tmp;
CREATE TABLE web.user_stitching_tmp
  DISTKEY(domain_userid)
  SORTKEY(domain_userid)
AS (

  -- WITH domainid_to_userid AS (
  --   SELECT
  --     ev.domain_userid,
  --     ev.user_id,
  --     MAX(ev.collector_tstamp) AS collector_tstamp

  --   FROM atomic.events AS ev
  --   WHERE ev.user_id IS NOT NULL
  --   AND ev.domain_userid IS NOT NULL
  --   GROUP BY 1,2
  -- )

  -- , latest_mapping AS (
  --   SELECT *,
  --     ROW_NUMBER() OVER (PARTITION BY domain_userid ORDER BY collector_tstamp) rank_collector_tstamp

  --     FROM domainid_to_userid
  --   )

  -- SELECT *

  -- FROM latest_mapping
  -- WHERE rank_collector_tstamp = 1 

  WITH sessions AS (
    SELECT
      ev.domain_userid,
      ev.user_id,
      ev.domain_sessionid AS session_id,
      MIN(ev.derived_tstamp) AS session_start_tmstamp

    FROM atomic.events AS ev
    WHERE ev.user_id IS NOT NULL
    AND ev.domain_userid IS NOT NULL
    GROUP BY 1,2,3
    )

  , user_id_lookback AS (
    SELECT
      s.domain_userid,
      s.user_id,
      s.session_start_tmstamp,
      LEAD(s.user_id) OVER(PARTITION BY s.domain_userid ORDER BY s.session_start_tmstamp) next_user_id,
      COALESCE(LEAD(s.session_start_tmstamp) OVER(PARTITION BY s.domain_userid ORDER BY s.session_start_tmstamp),
           NOW()) next_session_start_tmstamp --if last session ever take current date

    FROM sessions AS s
    )


  SELECT 
    lb.domain_userid,
    lb.user_id,
    lb.session_start_tmstamp AS valid_from,
    lb.next_session_start_tmstamp AS valid_to

  FROM user_id_lookback AS lb
  WHERE COALESCE(lb.user_id, 0) != COALESCE(lb.next_user_id, 0) --COALESCE to account of NULL user_id for latest value.
  );


  
