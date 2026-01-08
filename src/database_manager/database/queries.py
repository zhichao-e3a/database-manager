REBUILD_HISTORICAL = """
SELECT
uu.mobile,
rr.id,
rr.start_ts,
rr.start_test_ts,
rr.contraction_url,
rr.hb_baby_url,
rr.raw_fetal_url,
rr.basic_info,
rr.conclusion,
dd.end_born_ts,
rr.utime
FROM extant_future_user.user AS uu
INNER JOIN extant_future_data.origin_data_record AS rr ON uu.id = rr.user_id
INNER JOIN extant_future_user.user_detail AS dd ON uu.id = dd.uid
AND rr.contraction_url <> ''
AND rr.hb_baby_url <> ''
AND dd.end_born_ts IS NOT NULL
AND dd.end_born_ts <> 0
"""

RECRUITED = """
SELECT
uu.mobile,
rr.id,
rr.start_ts,
rr.start_test_ts,
rr.contraction_url,
rr.hb_baby_url,
rr.raw_fetal_url,
rr.basic_info,
rr.conclusion,
rr.utime
FROM extant_future_user.user AS uu
INNER JOIN origin_data_record AS rr ON uu.id = rr.user_id
AND rr.contraction_url <> ''
AND rr.hb_baby_url <> ''
AND uu.mobile IN ({numbers})
AND rr.start_ts BETWEEN UNIX_TIMESTAMP({start}) AND UNIX_TIMESTAMP({end})
"""

HISTORICAL_METADATA_QUERY =  """
SELECT
hp.mobile,
dd.age,
dd.height,
dd.old_weight,
mm.record_type,
mm.record_answer,
dd.expected_born_date,
dd.end_born_ts
FROM (
    SELECT DISTINCT
    uu.mobile,
    uu.id AS user_id
    FROM extant_future_user.user AS uu
    INNER JOIN extant_future_data.origin_data_record AS rr ON uu.id = rr.user_id
    INNER JOIN extant_future_user.user_detail AS dd ON uu.id = dd.uid
    WHERE
    rr.contraction_url <> ''
    AND rr.hb_baby_url <> ''
    AND dd.end_born_ts IS NOT NULL
    AND dd.end_born_ts <> 0
) AS hp
JOIN extant_future_user.user_detail AS dd ON hp.user_id = dd.uid
LEFT JOIN extant_future_user.medical_record AS mm ON dd.uid = mm.user_id
AND mm.record_type IN (1, 2, 4, 5, 8, 13)
"""

RECRUITED_PATIENTS_QUERY = """
SELECT
uu.mobile,
FROM_UNIXTIME(rr.start_ts) AS m_time,
rr.basic_info,
rr.conclusion
FROM extant_future_user.user AS uu
JOIN extant_future_data.origin_data_record AS rr ON uu.id = rr.user_id
AND uu.mobile IN ({numbers})
AND rr.start_ts BETWEEN UNIX_TIMESTAMP({start}) AND UNIX_TIMESTAMP({end})
"""

HISTORICAL_PATIENTS_QUERY = """
SELECT
uu.name,
u.mobile,
uu.age,
oo.earliest,
oo.latest,
oo.basic_info,
oo.conclusion,
uu.height,
uu.old_weight,
uu.expected_born_date AS edd,
mm.record_type,
mm.record_answer
FROM extant_future_user.user AS u
JOIN extant_future_user.user_detail AS uu ON u.id = uu.uid
JOIN
(
	SELECT
	o1.user_id,
	FROM_UNIXTIME(o1.earliest) AS earliest,
	FROM_UNIXTIME(o1.latest) AS latest,
	o2.basic_info,
	o2.conclusion
	FROM
	(
		SELECT
		user_id,
		MIN(start_ts) AS earliest,
		MAX(start_ts) AS latest
		FROM extant_future_data.origin_data_record
		GROUP BY user_id
	) AS o1
	JOIN
	(
		SELECT user_id, start_ts, basic_info, conclusion
		FROM extant_future_data.origin_data_record
	) AS o2
	ON o1.user_id = o2.user_id AND o1.earliest = o2.start_ts
) AS oo ON uu.uid = oo.user_id
LEFT JOIN extant_future_user.medical_record AS mm ON oo.user_id = mm.user_id AND mm.record_type IN (1, 2, 4, 5, 8, 13)
WHERE
u.mobile
IN
({numbers})
"""