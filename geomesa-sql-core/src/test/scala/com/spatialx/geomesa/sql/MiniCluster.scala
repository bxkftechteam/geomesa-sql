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

import java.io.{File, FileWriter}
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloConfig}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

case object MiniCluster extends LazyLogging {

  private val miniClusterTempDir = Files.createTempDirectory("geomesa-sql-mini-acc-")

  val namespace = "geomesa_sql"

  lazy val cluster: MiniAccumuloCluster = {
    logger.info(s"Starting Accumulo minicluster at $miniClusterTempDir")
    val config = new MiniAccumuloConfig(miniClusterTempDir.toFile, Users.root.password)
    sys.props.get("geomesa.accumulo.test.tablet.servers").map(_.toInt).foreach(config.setNumTservers)
    val cluster = new MiniAccumuloCluster(config)
    // required for zookeeper 3.5
    WithClose(new FileWriter(new File(miniClusterTempDir.toFile, "conf/zoo.cfg"), true)) { writer =>
      writer.write("admin.enableServer=false\n") // disable the admin server, which tries to bind to 8080
      writer.write("4lw.commands.whitelist=*\n") // enable 'ruok', which the minicluster uses to check zk status
    }
    cluster.start()

    // Create namespace for testing
    val client = cluster.createAccumuloClient(Users.root.name, new PasswordToken(Users.root.password))
    client.namespaceOperations().create(namespace)
    client.close()
    logger.info("Started Accmulo minicluster")

    // Done
    cluster
  }

  sys.addShutdownHook({
    logger.info("Stopping Accumulo minicluster")
    try { cluster.stop() } finally {
      PathUtils.deleteRecursively(miniClusterTempDir)
    }
    logger.info("Stopped Accumulo minicluster")
  })

  case class UserWithAuths(name: String, password: String)

  object Users {
    val root  = UserWithAuths("root", "secret")
  }
}
