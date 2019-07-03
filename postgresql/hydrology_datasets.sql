BEGIN;

DROP TABLE IF EXISTS big_vector.hydrology_1;

CREATE TABLE big_vector.hydrology_1(
gid integer,
resolution integer,
gnis_name text,
reachcode text,
ftype integer,
fcode integer,
visibilityfilter integer,
geom geometry
);

DROP TABLE IF EXISTS big_vector.hydrology_2;

CREATE TABLE big_vector.hydrology_2(
gid integer,
resolution integer,
gnis_name text,
reachcode text,
ftype integer,
fcode integer,
visibilityfilter integer,
geom geometry
);

DROP TABLE IF EXISTS big_vector.hydrology_3;

CREATE TABLE big_vector.hydrology_3(
gid integer,
resolution integer,
gnis_name text,
reachcode text,
ftype integer,
fcode integer,
visibilityfilter integer,
geom geometry
);

END;