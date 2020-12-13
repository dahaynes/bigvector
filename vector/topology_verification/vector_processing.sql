ALTER TABLE big_vector.dc ADD COLUMN valid varchar(4);

UPDATE big_vector.dc
SET valid = ST_IsValid(geom);

UPDATE big_vector.dc
SET geom = ST_MakeValid(geom)
WHERE ST_ISValid(geom) IS NOT TRUE;

UPDATE big_vector.dc
SET valid = ST_IsValid(geom);


ALTER TABLE big_vector.pennsylvania drop COLUMN valid ;varchar(4);
-- SELECT *
-- FROM big_vector.dc

-- LIMIT 5