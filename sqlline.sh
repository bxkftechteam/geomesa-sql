#!/usr/bin/env bash

java -Duser.timezone=UTC \
     -Dcalcite.default.charset=utf8 \
     -jar geomesa-sql-cli/target/geomesa-sql-cli_2.12-0.1.0-SNAPSHOT.jar \
     --verbose=true \
     -u "jdbc:calcite:model=experi/model-alihbase.yaml;fun=spatial;caseSensitive=false;lex=MYSQL" \
     -n root -p root
