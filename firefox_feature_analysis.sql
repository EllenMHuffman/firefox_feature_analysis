--FIREFOX DATASET, PROJECT 2

--IDENTIFY THE PROBLEM
--Do our power users have higer use of the bookmarks or tabs features?


--Parameters/Assumptions:
--	Data from recent test flight: customer surveys, usage analytics
--	Test was globla; lasted 7 days
--  Using data from users who "only use" or "primarily use" Firefox



--UNDERSTANDING THE SAMPLE
SELECT * FROM events LIMIT 10;
--user_id, event_code, data1, data2, data3, timestamp, 
--	session_id (not used)
SELECT * FROM survey LIMIT 10;
--user_id, q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13,
--	q14 (not used)
--Note: not all users completed a survey
SELECT * FROM users LIMIT 10;
--id, fx_version, os, version, number_extensions, 
--	survey_answers (not used), location (not used)

--How the tables connect:
survey.user_id = users.id = events.user_id

--Overview of users OS
WITH os_count AS
(SELECT
  id
, CASE WHEN os ILIKE '%windows%' then 1 else 0 end AS windows
, CASE WHEN os ILIKE '%mac%' then 1 else 0 end AS mac
, CASE WHEN os ILIKE '%linux%' OR os ILIKE '%sunos%' then 1 else 0 
	end AS linux
FROM users
)
SELECT
  SUM(windows) AS windows_count
, SUM(mac) AS mac_count
, SUM(linux) AS linux_count
FROM os_count;

--Type of OS of users who completed survey
WITH os_count AS
(SELECT
  id
, CASE WHEN os ILIKE '%windows%' then 1 else 0 end AS windows
, CASE WHEN os ILIKE '%mac%' then 1 else 0 end AS mac
, CASE WHEN os ILIKE '%linux%' OR os ILIKE '%sunos%' then 1 else 0
	end AS linux
FROM users u
  INNER JOIN survey s ON u.id=s.user_id
WHERE s.q2::INT = 0 OR s.q4::INT = 1
)
SELECT
  SUM(windows) AS windows_count
, SUM(mac) AS mac_count
, SUM(linux) AS linux_count
FROM os_count;




--STEP 1: Build temp tables for analysis:
--	Event/Survey tables with only Firefox users
--	Session table for length of browsing
--	Power users table to compare bookmarks vs. tabs
--		(Power users must meet 2 of the 3 qualifiers)
DROP TABLE IF EXISTS ff_events;
SELECT
  e.*
INTO TEMP TABLE ff_events
FROM events e
  INNER JOIN survey s ON e.user_id = s.user_id
WHERE s.q2::INT = 0 OR s.q4::INT = 1
;
UPDATE ff_events SET data1 = NULL WHERE data1 = '';

DROP TABLE IF EXISTS ff_survey;
SELECT
  s.*
INTO TEMP TABLE ff_survey
FROM survey s
WHERE (s.q2::INT = 0 OR s.q4::INT = 1) 
;

DROP TABLE IF EXISTS sessions; 
SELECT
  user_id
, ROUND(SUM(ABS(end_time-adj_start_time))/60000, 2) AS minutes
INTO TEMP TABLE sessions
FROM (
	SELECT * 
	, CASE 
	    WHEN (start_time IS NULL AND code_1 = 0) 
	    	THEN 1289278677174
	    WHEN (start_time IS NULL AND code_1 = 2) 
	    	THEN 1289252829083
	    ELSE start_time END AS adj_start_time
	FROM (
		SELECT 
		  a.user_id
		, a.rownum
		, a.event_code AS code_1
		, a.timestamp AS end_time
		, a.max_time AS max_time
		, b.event_code AS code_2
		, b.timestamp AS start_time
		FROM
			(SELECT
			  user_id
			, event_code
			, timestamp
			, row_number () OVER (PARTITION BY user_id 
				ORDER BY timestamp) AS rownum
			, MAX(timestamp) OVER (PARTITION BY user_id) 
				AS max_time
			FROM ff_events
			WHERE event_code IN (0,2)
			) a
		LEFT JOIN 
			(SELECT
			  user_id
			, event_code
			, timestamp
			, row_number () OVER (PARTITION BY user_id 
				ORDER BY timestamp) AS rownum
			FROM ff_events
			WHERE event_code IN (0,2)
			) b 
		  ON a.user_id = b.user_id AND a.rownum = b.rownum+1
		ORDER BY 1
	) AS s
	WHERE code_1 = 2 OR (end_time = max_time AND rownum = 1)
	ORDER BY 1
	) AS x
GROUP BY 1
;

DROP TABLE IF EXISTS power_users;
SELECT *
INTO TEMP TABLE power_users
FROM
	(
	SELECT
	  s.user_id
	, MAX(CASE WHEN event_code = 8 THEN REPLACE(data1, 
		' total bookmarks', '') ELSE NULL END) AS bookmarks
	, SUM(CASE WHEN event_code IN (9, 10, 11) THEN 1 ELSE 0 END) 
		AS bookmark_events
	, ROUND(AVG(CASE WHEN event_code = 26 THEN REPLACE(data2, 
		' tabs','')::INT ELSE NULL END), 2) AS avg_tabs
	, MAX(CASE WHEN event_code = 26 THEN REPLACE(data2, 
		' tabs','')::INT ELSE NULL END) AS max_tabs
	, MAX(CASE WHEN event_code = 23 THEN data1 ELSE NULL END) 
		AS history
	, ROUND(MAX(t.minutes), 2) AS minutes 
	, MAX(s.q7) AS q7
	FROM ff_survey s
	  INNER JOIN ff_events e ON s.user_id = e.user_id
	  INNER JOIN sessions t ON e.user_id = t.user_id
	group by 1
	) AS x
WHERE
  (history::INT > 500 AND minutes >45) OR
  (minutes >45 AND q7::INT >1) OR
  (q7::INT > 1 AND history::INT > 500)




--STEP 2: Define "Qualifiers" for power users

-- #1 Page views (using History as proxy)
WITH history_count AS
	(SELECT
	  user_id
	, AVG(data1::INT) AS history
	FROM ff_events
	WHERE event_code = 23
	GROUP BY 1)
SELECT
  ROUND(MIN(history), 0) AS min_history
, ROUND(MAX(history), 0 ) AS max_history
, ROUND(AVG(history), 2) AS avg_history
, ROUND(STDDEV_SAMP(history), 2) AS stddev_history
FROM history_count
;

WITH hist_count AS
	(SELECT
	  user_id
	, AVG(data1::INT) AS history
	FROM ff_events
	WHERE event_code = 23
	GROUP BY 1)
SELECT
  COUNT(*)
FROM hist_count
WHERE history <500


-- #2 Self-reported web use (Q7 on survey)
Select 
  MIN(q7)
, MAX(q7)
, ROUND(AVG(q7::INT), 2)
, ROUND(STDDEV_SAMP(q7::INT), 2)
FROM ff_survey 
;

-- #3 Session length
--	Fill in missing start/end times in sessions temp table
	SELECT MIN(timestamp) FROM events
	-- 1289252829083
	SELECT MAX(timestamp) FROM events
	-- 1289278677174

--  Calculate total browser time in minutes per user:
SELECT
  user_id
, ROUND(SUM(ABS(end_time-adj_start_time))/60000, 2) AS minutes
FROM sessions
GROUP BY 1
ORDER BY 1
;

--  Measures of center for session length:
SELECT
  ROUND(MIN(minutes), 2) AS min_minutes
, ROUND(MAX(minutes), 2) AS max_minutes
, ROUND(AVG(minutes), 2) AS avg_minutes
, ROUND(STDDEV_SAMP(minutes), 2) AS stddev_minutes
FROM sessions
;




--STEP 3: Define Measures of Success for Tabs and Bookmarks
--number of tabs
WITH tab_count AS
	(SELECT
	  user_id
	, AVG(REPLACE(data2, ' tabs', '')::INT) AS tabs
	FROM ff_events
	WHERE event_code = 26
	GROUP BY 1)
SELECT
  ROUND(MIN(tabs), 0) AS min_tab
, ROUND(MAX(tabs), 0 ) AS max_tab
, ROUND(AVG(tabs), 2) AS avg_tab
, ROUND(STDDEV_SAMP(tabs), 2) AS stddev_tab
FROM tab_count
;

--number of bookmarks
WITH b_count AS
	(SELECT
	  user_id
	, MAX(CASE WHEN event_code = 8 THEN REPLACE(data1, 
		' total bookmarks', '') ELSE NULL END) AS bookmarks
	, SUM(CASE WHEN event_code IN (9, 10, 11) THEN 1 ELSE 0 END) 
		AS bookmark_events
	FROM ff_events
	GROUP BY 1)
SELECT
  ROUND(MIN(bookmarks::INT), 0) AS min_b
, ROUND(MAX(bookmarks::INT), 0 ) AS max_b
, ROUND(AVG(bookmarks::INT), 2) AS avg_b
, ROUND(STDDEV_SAMP(bookmarks::INT), 2) AS stddev_b
FROM b_count
;

WITH b_count AS
	(SELECT
	  user_id
	, MAX(CASE WHEN event_code = 8 THEN REPLACE(data1, 
		' total bookmarks', '') ELSE NULL END) AS bookmarks
	, SUM(CASE WHEN event_code IN (9, 10, 11) THEN 1 ELSE 0 END) 
		AS bookmark_events
	FROM ff_events
	GROUP BY 1)
SELECT
  ROUND(MIN(bookmark_events::INT), 0) AS min_be
, ROUND(MAX(bookmark_events::INT), 0 ) AS max_be
, ROUND(AVG(bookmark_events::INT), 2) AS avg_be
, ROUND(STDDEV_SAMP(bookmark_events::INT), 2) AS stddev_be
FROM b_count
;




--STEP 4: Use power_users table to compare usage of tabs
--	and bookmarks against success metrics (1290 users total)

SELECT 
  COUNT(CASE WHEN bookmark_events =0 THEN 'bookmark_events' 
	ELSE NULL END) as low_be
, COUNT(CASE WHEN bookmark_events >0 AND bookmark_events <4 
	THEN 'bookmark_events' ELSE NULL END) as med_be
, COUNT(CASE WHEN bookmark_events >=4 THEN 'bookmark_events' 
	ELSE NULL END) as high_be
FROM power_users
;

SELECT
  COUNT(CASE WHEN bookmarks IS NULL THEN 1 ELSE null end) AS low_b
, COUNT(CASE WHEN bookmarks::INT <75 THEN 'bookmarks' 
	ELSE NULL END) AS med_b
, COUNT(CASE WHEN bookmarks::INT >=75 THEN 'bookmarks' 
	ELSE NULL END) AS high_b
FROM power_users
;

SELECT
  COUNT(CASE WHEN avg_tabs <6 THEN 'avg_tabs' 
	ELSE NULL END) as low_tab
, COUNT(CASE WHEN avg_tabs >5 AND avg_tabs <10 
	THEN 'avg_tabs' ELSE NULL END) as med_tab
, COUNT(CASE WHEN avg_tabs >=10 THEN 'avg_tabs' 
	ELSE NULL END) as high_tab
FROM power_users
;

--crashes compared to tabs
--max crashes per user = 66; 10596 users experienced a crash
SELECT
 user_id
, timestamp
, event_code
, data1
, data2
FROM events
WHERE event_code IN (3, 25, 26)
ORDER BY 1, 2
LIMIT 1000

SELECT
  COUNT(user_id)
FROM
(
SELECT
  user_id
, COUNT(*)
FROM events
WHERE data1 ILIKE '%crash%'
GROUP BY 1
ORDER BY 2 DESC
) AS crash_count


