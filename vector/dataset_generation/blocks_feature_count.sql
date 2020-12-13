WITH data as
(
SELECT count(gid) FROM big_vector.Vermont
UNION 
SELECT count(gid) FROM big_vector.Florida
UNION 
SELECT count(gid) FROM big_vector.California
UNION 
SELECT count(gid) FROM big_vector.Delaware 
UNION 
SELECT count(gid) FROM big_vector.Louisiana  
UNION 
SELECT count(gid) FROM big_vector.WestVirginia  
UNION 
SELECT count(gid) FROM big_vector.Montana 
UNION 
SELECT count(gid) FROM big_vector.Oklahoma 
UNION 
SELECT count(gid) FROM big_vector.Mississippi 
UNION 
SELECT count(gid) FROM big_vector.Wyoming 
UNION 
SELECT count(gid) FROM big_vector.Maryland 
UNION 
SELECT count(gid) FROM big_vector.SouthCarolina 
UNION 
SELECT count(gid) FROM big_vector.Washington
UNION 
SELECT count(gid) FROM big_vector.Michigan
UNION 
SELECT count(gid) FROM big_vector.Arkansas
UNION 
SELECT count(gid) FROM big_vector.NewHampshire
UNION 
SELECT count(gid) FROM big_vector.Virginia
UNION 
SELECT count(gid) FROM big_vector.Idaho
UNION 
SELECT count(gid) FROM big_vector.Kentucky
UNION 
SELECT count(gid) FROM big_vector.Maine
UNION 
SELECT count(gid) FROM big_vector.Pennsylvania
UNION 
SELECT count(gid) FROM big_vector.Utah
UNION 
SELECT count(gid) FROM big_vector.NewJersey
UNION 
SELECT count(gid) FROM big_vector.Nebraska
UNION 
SELECT count(gid) FROM big_vector.Ohio
UNION 
SELECT count(gid) FROM big_vector.NorthDakota
UNION 
SELECT count(gid) FROM big_vector.NorthCarolina
UNION 
SELECT count(gid) FROM big_vector.Connecticut
UNION 
SELECT count(gid) FROM big_vector.RhodeIsland
UNION 
SELECT count(gid) FROM big_vector.Nevada
UNION 
SELECT count(gid) FROM big_vector.Texas
UNION 
SELECT count(gid) FROM big_vector.DC
UNION 
SELECT count(gid) FROM big_vector.Kansas
UNION 
SELECT count(gid) FROM big_vector.Tennessee
UNION 
SELECT count(gid) FROM big_vector.NewYork
UNION 
SELECT count(gid) FROM big_vector.Minnesota
UNION 
SELECT count(gid) FROM big_vector.SouthDakota
UNION 
SELECT count(gid) FROM big_vector.Iowa
UNION 
SELECT count(gid) FROM big_vector.Georgia
UNION 
SELECT count(gid) FROM big_vector.NewMexico
UNION 
SELECT count(gid) FROM big_vector.Alabama
UNION 
SELECT count(gid) FROM big_vector.Arizona
UNION 
SELECT count(gid) FROM big_vector.Wisconsin
UNION 
SELECT count(gid) FROM big_vector.Missouri
UNION 
SELECT count(gid) FROM big_vector.Indiana
UNION 
SELECT count(gid) FROM big_vector.Illinois
UNION 
SELECT count(gid) FROM big_vector.Colorado
UNION 
SELECT count(gid) FROM big_vector.Massachusetts
UNION 
SELECT count(gid) FROM big_vector.Oregon
)
SELECT sum(count) as total_features
FROM data