/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spatialx.geomesa.sql.example;

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example of running SQL queries on GeoMesa DataStore using GeoMesa SQL JDBC
 * module.
 */
public class Example {
  public static void main(String[] args) throws SQLException {
    if (args.length < 1) {
      System.out.println("Usage: java -jar geomesa-sql-example.jar <jdbcUrl>");
      System.out.println("  Example: java -jar geomesa-sql-example.jar " +
                         "\"jdbc:geomesa:accumulo.catalog=geomesa.test;accumulo.instance.id=geomesa;" +
                         "accumulo.zookeepers=localhost:2181;accumulo.user=root;accumulo.password=secret\"");
      System.exit(1);
    }

    String jdbcUrl = args[0];
    Console console = System.console();
    Properties info = new Properties();
    info.setProperty("fun", "spatial");
    info.setProperty("caseSensitive", "false");
    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {
      while (true) {
        console.printf("> ");
        console.flush();
        String sql = console.readLine();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
          int numColumns = resultSet.getMetaData().getColumnCount();
          while (resultSet.next()) {
            console.printf("|");
            for (int k = 1; k <= numColumns; k++) {
              Object datum = resultSet.getObject(k);
              console.printf("%s|", datum);
            }
            console.printf("\n");
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
