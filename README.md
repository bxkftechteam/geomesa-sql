# GeoMesa SQL

A SQL layer built upon GeoMesa DataStore.

> This project is still under construction.

## Try it out

### Build

```
mvn clean package
```

### Run

```
./sqlline.sh
```

Then you are free to query GeoMesa schemas using SQL.

#### Listing tables

```
0: jdbc:calcite:model=src/test/resources/mode> !table
!table
+-----------+-------------+--------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
| TABLE_CAT | TABLE_SCHEM |  TABLE_NAME  |  TABLE_TYPE  | REMARKS | TYPE_CAT | TYPE_SCHEM | TYPE_NAME | SELF_REFERENCING_COL_NAME | REF_GENERATION |
+-----------+-------------+--------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
|           | GEOMESA     | BANK         | TABLE        |         |          |            |           |                           |                |
|           | GEOMESA     | TEST_SCHEMA  | TABLE        |         |          |            |           |                           |                |
|           | GEOMESA     | TEST_SCHEMA2 | TABLE        |         |          |            |           |                           |                |
|           | GEOMESA     | TEST_TRAJ    | TABLE        |         |          |            |           |                           |                |
|           | metadata    | COLUMNS      | SYSTEM TABLE |         |          |            |           |                           |                |
|           | metadata    | TABLES       | SYSTEM TABLE |         |          |            |           |                           |                |
+-----------+-------------+--------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
```

#### Querying data

```
0: jdbc:calcite:model=src/test/resources/mode> SELECT * FROM TEST_SCHEMA;
SELECT * FROM TEST_SCHEMA;
+--------------------------------------+-------------------+
|               __FID__                |        PT         |
+--------------------------------------+-------------------+
| d0aa047c-98b7-4b42-8582-4c8590348ce3 | {"x":-10,"y":-20} |
| 00aa047c-98b7-4b42-8221-e3239b3c5b0d | {"x":-10,"y":-20} |
| 60aa047c-98b7-4b42-98a9-c00cc799fa4c | {"x":-10,"y":-20} |
| 70aa0711-2e6c-449d-9994-57184c358af8 | {"x":10,"y":20}   |
| f0aa0719-aa2e-468c-b38d-c14364081f17 | {"x":30,"y":40}   |
+--------------------------------------+-------------------+
5 rows selected (0.131 seconds)
```
