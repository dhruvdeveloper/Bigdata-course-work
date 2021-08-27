drop table Twitter;
DROP VIEW tempOutput;

create table Twitter (
  id int,
  fid int
  )
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Twitter;

CREATE VIEW tempOutput AS select fid, count(fid) AS fid2 from Twitter GROUP by fid;

SELECT fid2, count(fid2) FROM tempOutput GROUP by fid2;
