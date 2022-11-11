# Example of using GeoMesa SQL Programmatically

## Setup

GeoMesa SQL provides a JDBC compatible driver module for accessing GeoMesa
DataStores programatically using JDBC interface. You can add `geomesa-sql-jdbc`
to your project by adding the following dependency to your pom.xml:

```xml
<dependency>
  <groupId>io.github.bxkftechteam</groupId>
  <artifactId>geomesa-sql-jdbc_${scala.binary.version}</artifactId>
  <version>${geomesa.sql.version}</version>
</dependency>
```

Where `${scala.binary.version}` is the Scala version of your main project,
which should be consistent with the scala version of your geomesa datastore
dependencies. `${geomesa.sql.version}` is the version of GeoMesa SQL, you can
find the latest version in [Maven central](https://search.maven.org/search?q=geomesa-sql-jdbc).

## Connect to GeoMesa DataStore

You can get a `Connection` object backed by GeoMesa SQL by specifying a GeoMesa JDBC URL:

```java
String url = "jdbc:geomesa:accumulo.catalog=geomesa.test;fun=spatial";
Connection connection = DriverManager.getConnection(url);
```

GeoMesa JDBC URL starts with `jdbc:geomesa:`, followed by several GeoMesa
DataStore config properties and [Apache Calcite parameters](https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters).
For example, we need to specify `redis.catalog` and `redis.url` when connecting to GeoMesa Redis DataStore, and we'd like to enable
spatial functions of Apache Calcite. We can connect to the datastore using the following JDBC URL:

```java
String url = "jdbc:geomesa:redis.catalog=catalog;redis.url=localhost:6379;fun=spatial";
```

## Run SQL Statements

You can run SQL queries using the `Connection` object obtained in the previous
step, just like querying other SQL databases.

## Example Project

This directory is an example project for querying GeoMesa Accumulo DataStore using GeoMesa SQL.

You can build this project by running `mvn clean package`, then run the example command line tool:

```
java -jar target/geomesa-sql-example*.jar "jdbc:geomesa:accumulo.catalog=geomesa.test;accumulo.instance.id=geomesa;accumulo.zookeepers=localhost:2181;accumulo.user=root;accumulo.password=secret"
```

Please replace the Accumulo DataStore configs with the config of your own Accumulo cluster.
