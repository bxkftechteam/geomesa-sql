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

package com.spatialx.geomesa.sql

import org.apache.calcite.util.Sources
import org.specs2.execute.Result
import org.specs2.matcher.JUnitMustMatchers.ok
import org.specs2.matcher.MatchResult
import org.specs2.matcher.Matchers.{beNone, beSome, combineMatchResult, combineResult, empty, not}
import org.specs2.matcher.MustMatchers.theValue

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
 * Utilities for testing GeoMesa Calcite
 */
object GeoMesaCalciteTestUtils {

  def verifyResult(sql: String, model: String, refModel: String, isOrdered: Boolean = false, notEmpty: Boolean = false): MatchResult[Any] = {
    val resultArray = runSql(sql, model)
    val refResultArray = runSql(sql, refModel)
    if (isOrdered) compareOrderedResults(resultArray, refResultArray, notEmpty)
    else compareResults(resultArray, refResultArray, notEmpty)
  }

  def verifyPlan(sql: String, model: String, planPatterns: Seq[Regex], unexpectedPlanPatterns: Seq[Regex] = Seq.empty): Result = {
    val planSql = s"EXPLAIN PLAN FOR $sql"
    checkSql(planSql, model) { resultArray =>
      val plan = resultArray.map(row => row.mkString("|")).mkString("\n")
      println(s"Plan for query [$sql]:\n$plan")
      Result.foreach(planPatterns) { planPattern =>
        planPattern.findFirstMatchIn(plan) must beSome
      } and Result.foreach(unexpectedPlanPatterns) { planPattern =>
        planPattern.findFirstMatchIn(plan) must beNone
      }
    }
  }

  def runUpdate(sql: String, model: String): Long = {
    var connection: Connection = null
    var statement: Statement = null
    try {
      val info = new Properties
      if (model.startsWith("inline:")) {
        info.put("model", model)
      } else {
        info.put("model", Sources.of(classOf[DataTypeTest].getResource("/" + model)).file().getAbsolutePath)
      }
      info.put("fun", "spatial")
      info.put("caseSensitive", "false")
      connection = DriverManager.getConnection("jdbc:calcite:", info)
      statement = connection.createStatement()
      statement.executeUpdate(sql)
    } finally {
      Option(statement).foreach(_.close())
      Option(connection).foreach(_.close())
    }
  }

  def runSql(sql: String, model: String): Array[Array[AnyRef]] = {
    var connection: Connection = null
    var statement: Statement = null
    var resultSet: ResultSet = null
    try {
      val info = new Properties
      if (model.startsWith("inline:")) {
        info.put("model", model)
      } else {
        info.put("model", Sources.of(classOf[DataTypeTest].getResource("/" + model)).file().getAbsolutePath)
      }
      info.put("fun", "spatial")
      info.put("caseSensitive", "false")
      connection = DriverManager.getConnection("jdbc:calcite:", info)
      statement = connection.createStatement()
      resultSet = statement.executeQuery(sql)
      val columnCount = resultSet.getMetaData.getColumnCount
      val resultArray = ArrayBuffer.empty[Array[AnyRef]]
      while (resultSet.next()) {
        val line = Range(0, columnCount).map(k => resultSet.getObject(k + 1))
        resultArray.append(line.toArray)
      }
      resultArray.toArray
    } finally {
      Option(resultSet).foreach(_.close())
      Option(statement).foreach(_.close())
      Option(connection).foreach(_.close())
    }
  }

  def checkSql[R](sql: String, model: String)(checker: Array[Array[AnyRef]] => R): R = {
    val resultArray = runSql(sql, model)
    checker(resultArray)
  }

  def printResultSet(resultArray: Array[Array[AnyRef]]): MatchResult[Any] = {
    resultArray.foreach(row => println(row.mkString("|")))
    ok
  }

  def compareOrderedResults(resultArray: Array[Array[AnyRef]], refResultArray: Array[Array[AnyRef]], notEmpty: Boolean): MatchResult[Any] = {
    val result = resultArray.map(resultRowToString _)
    val refResult = refResultArray.map(resultRowToString _)
    if (notEmpty) (result must not be empty) and (result mustEqual refResult)
    else result mustEqual refResult
  }

  def compareResults(resultArray: Array[Array[AnyRef]], refResultArray: Array[Array[AnyRef]], notEmpty: Boolean): MatchResult[Any] = {
    val result = resultArray.map(resultRowToString _).sorted
    val refResult = refResultArray.map(resultRowToString _).sorted
    if (notEmpty) (result must not be empty) and (result mustEqual refResult)
    else result mustEqual refResult
  }

  private def resultRowToString(row: Array[AnyRef]): String = {
    row.map { case value =>
      value match {
        case buf: Array[Byte] if buf != null => buf.map("%02X" format _).mkString(" ")
        case _ => if (value != null) value.toString else "null"
      }
    }.mkString("|")
  }
}
