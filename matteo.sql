Copy 
(
SELECT idtrajectory, route.idterm, idtrace_o, idtrace_d, dataraw.latitude, dataraw.longitude, dataraw.timedate, tripdistance_m, triptime_s, checkcode, breaktime_s, c_idtrajectory
  FROM route
  inner join dataraw on dataraw.id=route.idtrace_o
  where date(dataraw.timedate)='2015-11-14' and (dataraw.longitude<13071021 and dataraw.longitude>11852593 and dataraw.latitude<42094757 and dataraw.latitude>41674405)
  order by route.idterm, dataraw.timedate
)
To 'D:\Utenti\Federico\matteo.csv' With CSV DELIMITER ';' HEADER;

