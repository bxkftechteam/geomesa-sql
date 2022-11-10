!outputformat vertical

!table

SELECT * FROM beijing_subway LIMIT 10;

SELECT * FROM beijing_subway_station LIMIT 10;

SELECT * FROM beijing_subway_station WHERE ST_Contains(ST_GeomFromText('POLYGON((116.42455179375989 39.92844818654511,116.5402515618263 39.92844818654511,116.5402515618263 39.88209595176308,116.42455179375989 39.88209595176308,116.42455179375989 39.92844818654511))'), geom);  

SELECT line, COUNT(1) FROM beijing_subway_station JOIN beijing_subway ON ST_Intersects(beijing_subway.geom, beijing_subway_station.geom) GROUP BY line;
