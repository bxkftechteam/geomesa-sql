[![Maven Central](https://img.shields.io/maven-central/v/io.github.bxkftechteam/geomesa-sql_2.12.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.bxkftechteam%22%20AND%20a:%22geomesa-sql_2.12%22)
[![Build and Test](https://github.com/bxkftechteam/geomesa-sql/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/bxkftechteam/geomesa-sql/actions/workflows/build.yml)
[![codecov](https://codecov.io/github/bxkftechteam/geomesa-sql/branch/main/graph/badge.svg?token=7ZFDI120CR)](https://codecov.io/github/bxkftechteam/geomesa-sql)

# GeoMesa SQL

Run SQL queries on GeoMesa DataStores, in the command line, or in your programs.

[![asciicast](https://asciinema.org/a/536798.svg)](https://asciinema.org/a/536798)

## Using GeoMesa SQL Programmatically

GeoMesa SQL provides a package for accessing GeoMesa DataStores using JDBC
interface. Please refer to the [example project](/example) for details.

## Run SQL using Command Line Tool

GeoMesa SQL comes up with a command line tool `geomesa-sqlline`, which is a
[sqlline](https://github.com/julianhyde/sqlline) based utility for running SQL
queries on GeoMesa DataStores. It has been tested on the following datastores:

* Accumulo
* HBase
* Cassandra
* Redis
* FileSystem

### Prerequisites

You need a working [GeoMesa command line tool](http://www.geomesa.org/documentation/stable/user/cli/index.html) for
accessing your GeoMesa DataStore. GeoMesa SQL has been tested with GeoMesa 3.4.1, other versions may also work.

### Download the binary distribution

You can download the binary distribution from
[Releases](https://github.com/bxkftechteam/geomesa-sql/releases). Please make
sure that the scala version of GeoMesa SQL matches with the GeoMesa command
line tool you are using.

### Setup `GEOMESA_HOME` environment variable

GeoMesa SQL command line tool needs jars and config files in GeoMesa command
line tool. `GEOMESA_HOME` must be configured properly before using
`geomesa-sqlline` command. You can configure `GEOMESA_HOME` in
`geomesa-sql/conf/geomesa-sql-env.sh`, or setting it temporarily in current
session by running `export GEOMESA_HOME=/path/to/geomesa`.

### Using GeoMesa SQL command line tool

You can run GeoMesa SQL command line tool using this command:

```
geomesa-sqlline -c <catalog> [-p <datastore_params>]
```

For example, to access GeoMesa Accumulo or HBase DataStore in catalog `geomesa`
using default datastore configuration, we can run the following command:

```
geomesa-sqlline -c geomesa
```

If you're accessing GeoMesa DataStore requiring more parameters other than
catalog, you can specify additional datastore parameters using `-p`. For
example, we need to specify `cassandra.contact.point` and `cassandra.keyspace`
when accessing GeoMesa Cassandra:

```
geomesa-sqlline -c geomesa -p "cassandra.contact.point=localhost:9042;cassandra.keyspace=geomesa"
```

`geomesa-sqlline` will bring you an interactive shell to run SQL queries. You can
list tables in the catalog using `!table`:

```
0: jdbc:geomesa:cassandra.catalog=geomesa> !table
+-----------+-------------+------------------------+--------------+---------+----------+------------+
| TABLE_CAT | TABLE_SCHEM |       TABLE_NAME       |  TABLE_TYPE  | REMARKS | TYPE_CAT | TYPE_SCHEM |
+-----------+-------------+------------------------+--------------+---------+----------+------------+
|           | GEOMESA     | beijing_subway         | TABLE        |         |          |            |
|           | GEOMESA     | beijing_subway_station | TABLE        |         |          |            |
|           | GEOMESA     | starbucks              | TABLE        |         |          |            |
|           | metadata    | COLUMNS                | SYSTEM TABLE |         |          |            |
|           | metadata    | TABLES                 | SYSTEM TABLE |         |          |            |
+-----------+-------------+------------------------+--------------+---------+----------+------------+
```

List columns in a table using `!describe "<table_name>"`:

```
0: jdbc:geomesa:cassandra.catalog=geomesa> !describe "starbucks"
+-----------+-------------+------------+----------------+-----------+-------------------------------+
| TABLE_CAT | TABLE_SCHEM | TABLE_NAME |  COLUMN_NAME   | DATA_TYPE |           TYPE_NAME           |
+-----------+-------------+------------+----------------+-----------+-------------------------------+
|           | GEOMESA     | starbucks  | __FID__        | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | id_0           | 4         | INTEGER                       |
|           | GEOMESA     | starbucks  | brand          | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | store_name     | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | ownership_type | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | address        | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | city           | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | state_province | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | country        | 12        | VARCHAR CHARACTER SET "UTF-8" |
|           | GEOMESA     | starbucks  | postcode       | 4         | INTEGER                       |
|           | GEOMESA     | starbucks  | geog           | 2015      | GEOMETRY                      |
+-----------+-------------+------------+----------------+-----------+-------------------------------+
```

Run SQL queries:

```
0: jdbc:geomesa:cassandra.catalog=geomesa> SELECT store_name, ownership_type, geog FROM starbucks WHERE ownership_type = 'Licensed' LIMIT 10;
+-------------------------------------+----------------+-----------------------------+
|             STORE_NAME              | OWNERSHIP_TYPE |            GEOG             |
+-------------------------------------+----------------+-----------------------------+
| Safeway-Ewa #2897                   | Licensed       | POINT (-158.01933 21.33606) |
| Hilton Hawaiian Village-Kalia Tower | Licensed       | POINT (-157.83602 21.28403) |
| Target Kailua T-2697                | Licensed       | POINT (-157.7407 21.39112)  |
| Schofield Barracks Main Store Mall  | Licensed       | POINT (-158.07508 21.47854) |
| Safeway-Lihue #2894                 | Licensed       | POINT (-159.38637 21.96841) |
| Safeway - Kihei #1500               | Licensed       | POINT (-156.449 20.75245)   |
| Marriott's Maui Ocean Club          | Licensed       | POINT (-156.69526 20.9167)  |
| Ralphs-Palos Verdes #720            | Licensed       | POINT (-118.40603 33.74952) |
| Sodexo@Naval Station 32nd. St. San  | Licensed       | POINT (-117.12923 32.69084) |
| Vons-Chula Vista #2071              | Licensed       | POINT (-116.9393 32.64391)  |
+-------------------------------------+----------------+-----------------------------+
10 rows selected (0.066 seconds)
```

Run SQL queries with spatial functions:

```
0: jdbc:geomesa:cassandra.catalog=geomesa> SELECT store_name, geog FROM starbucks WHERE ST_Within(geog, ST_GeomFromText('POLYGON((-71.78681142189922 42.84501461908715,-70.90446657994212 41.33088304889827,-69.63607675888956 42.76409278843215,-71.78681142189922 42.84501461908715))')) LIMIT 10;
+------------------------------+----------------------------+
|          STORE_NAME          |            GEOG            |
+------------------------------+----------------------------+
| Taunton Depot                | POINT (-71.0667 41.87916)  |
| Target Easton T-2267         | POINT (-71.14357 42.02736) |
| Brockton, Westgate Drive     | POINT (-71.0543 42.09391)  |
| Stop & Shop-Plymouth #469    | POINT (-70.68628 41.95501) |
| Hingham, Rte. 53             | POINT (-70.9013 42.18106)  |
| Target Taunton T-1189        | POINT (-71.06911 41.8783)  |
| Target Plainville T-1930     | POINT (-71.31021 42.03397) |
| Medfield, Main St & North St | POINT (-71.30541 42.18706) |
| Target Wareham T-2292        | POINT (-70.74462 41.7746)  |
| Brockton, Belmont Street     | POINT (-71.06865 42.05886) |
+------------------------------+----------------------------+
10 rows selected (0.095 seconds)
```

If you want to query tables scattered in multiple catalogs, you can write a
[Calcite model file](https://calcite.apache.org/docs/model.html) and run
`geomesa-sql -m /path/to/calcite-model-file.yaml`. Here is an example of
Calcite model file:

```yaml
version: 1.0
defaultSchema: catalog1
schemas:
  - name: catalog1
    type: custom
    factory: com.spatialx.geomesa.sql.GeoMesaSchemaFactory
    operand:
      # Options for connecting to GeoMesa DataStore
      "hbase.catalog": "geomesa_catalog1"

  - name: catalog2
    type: custom
    factory: com.spatialx.geomesa.sql.GeoMesaSchemaFactory
    operand:
      "hbase.catalog": "geomesa_catalog2"
```

## Building from Source

Requirements:

* [Git](http://git-scm.com/)
* [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Maven](http://maven.apache.org/) 3.6.3 or later

Run this command to download the source repository and build this project:

```
git clone git@github.com:bxkftechteam/geomesa-sql.git
cd geomesa-sql
mvn clean package
```

This will build the artifacts and run unit tests using a mini Accumulo
cluster. You can run `mvn clean package -DskipTests` to skip running tests. The
distribution package containing the `geomesa-sqlline` tool will be generated in
`geomesa-sql-dist/target` directory.

This project depends on Scala 2.12 by default. You can run
`./build/change-scala-version.sh 2.11` to switch dependency to Scala 2.11.

After building the project from source, you can test and debug
`geomesa-sqlline` directly in `geomesa-sql-cli` directory.

```
cd geomesa-sql-cli
GEOMESA_HOME=/path/to/geomesa ./bin/geomesa-sqlline -c <catalog>
```

## Contributing

This project is still in its early stage, issues and pull requests are
welcomed. If you are interested in contributing to this project please read the
[**Contribution Guide**](CONTRIBUTING.md).
