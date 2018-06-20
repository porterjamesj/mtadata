-- WITH depart_train AS
--   (SELECT * from mta.stop_times
--    where stop_mta_id = 'A24'
--    and direction = 'N'
--    and timestamp > '2018-05-08 13:10:00-4'
--    and trip_mta_id like '%A..%'
--    order by timestamp limit 1)
-- SELECT * FROM mta.stop_times
-- WHERE timestamp > depart_train.timestamp
-- and stop_mta_id = 'A15'
-- and trip_mta_id = depart_train.trip_mta_id;

SELECT finish_train.* from mta.stop_times as finish_train JOIN mta.stop_times as depart_train
ON finish_train.trip_mta_id = depart_train.trip_mta_id AND finish_train.timestamp > depart_train.timestamp
WHERE depart_train.stop_mta_id = 'A24'
AND finish_train.stop_mta_id = 'A15'
AND depart_train.timestamp > '2018-05-08 13:10:00-4'
AND depart_train.direction = 'N'
AND depart_train.trip_mta_id like '%A..%'
ORDER BY finish_train.timestamp
LIMIT 1;
