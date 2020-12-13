CREATE Table us_blocks as

SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Vermont
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Florida
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.California
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Delaware 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Louisiana  
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.WestVirginia  
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Montana 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Oklahoma 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Mississippi 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Wyoming 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Maryland 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.SouthCarolina 
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Washington
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Michigan
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Arkansas
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.NewHampshire
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Virginia
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Idaho
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Kentucky
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Maine
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Pennsylvania
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Utah
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.NewJersey
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Nebraska
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Ohio
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.NorthDakota
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.NorthCarolina
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Connecticut
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.RhodeIsland
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Nevada
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Texas
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.DC
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Kansas
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Tennessee
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.NewYork
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Minnesota
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Hawaii
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.SouthDakota
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Iowa
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Georgia
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.NewMexico
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Alabama
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Arizona
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Wisconsin
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Missouri
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Indiana
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Illinois
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Colorado
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Massachusetts
UNION 
SELECT gid, statefp10, countyfp10, tractce10, blockce, blockid10, pop10, geom, valid FROM big_vector.Oregon