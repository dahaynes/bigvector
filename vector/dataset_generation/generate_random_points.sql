CREATE TABLE big_vector.randpoints_100 (id bigint, geom geometry);
DROP TABLE IF EXISTS big_vector.randpoints_100;
with data as
(
SELECT ST_Dump(ST_GeneratePoints(geom, 10000100)) as dump
FROM big_vector.continent
)
INSERT INTO big_vector.randpoints_100
SELECT (dump).path[1] as id, (dump).geom as geom
FROM data
LIMIT 10000000