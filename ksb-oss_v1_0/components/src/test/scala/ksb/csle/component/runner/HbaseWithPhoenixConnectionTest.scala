package ksb.csle.component.runner

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, HBaseConfiguration }
import org.apache.logging.log4j.scala.Logging
import org.scalatest._
import org.scalatest.Assertions._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.PreparedStatement
import java.sql.Statement

import ksb.csle.common.base.UnitSpec

/**
 * A Test Class for PhoenixHbaseJdbcDriver
 */

class HbaseWithPhoenixConnectionTest extends UnitSpec with Logging {

  val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration create

  conf.set("hbase.zookeeper.quorum", "localhost")
  val admin: HBaseAdmin = new HBaseAdmin(conf)
  val tableName = "mytable"

  "A HBaseConnector" should "delete table" in {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
    assert(admin.tableExists(tableName) === false)
  }

  it should "create table" in {
    val tableDesc = new HTableDescriptor(Bytes.toBytes(tableName))
    val idsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("ids"))
    tableDesc.addFamily(idsColumnFamilyDesc)
    admin.createTable(tableDesc)
    assert(admin.tableExists(tableName) === true)
  }

  it should "insert some data in the table and get the rows" in {
    val dummyString = "one"
    val dummpyInput = Bytes.toBytes(dummyString)
    val table = new HTable(conf, tableName)
    val theput = new Put(Bytes.toBytes("rowkey1"))
    theput.add(Bytes.toBytes("ids"), Bytes.toBytes("id1"), dummpyInput)
    table.put(theput)
    val theget = new Get(Bytes.toBytes("rowkey1"))
    val result = table.get(theget)
    val dummyOutput = result.value()
    assert(dummpyInput === dummyOutput)
    assert(dummyString === Bytes.toString(dummyOutput))
  }

  "A HbasePhoenixConnector" should "read Hbase table with phoenix jdbcDriver" in {
    val jdbcDriver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val jdbcUrl = "jdbc:phoenix:localhost:2181/hbase"
    val dbtable = "test"
    val conn = DriverManager.getConnection(jdbcUrl)
    val stmt = conn.createStatement()

    stmt.executeUpdate(s"drop table if exists ${dbtable}")
    stmt.executeUpdate(s"create table ${dbtable} (mykey integer not null primary key, mycolumn varchar)")
    stmt.executeUpdate(s"upsert into ${dbtable} values (1,'Hello')")
    stmt.executeUpdate(s"upsert into ${dbtable} values (2,'World!')")
    conn.commit()

    val normalSelSql = s"select * from ${dbtable}"
    val rs = stmt.executeQuery(normalSelSql)
    rs.next()
    assert(rs.getString("mycolumn") === "Hello")
    rs.next()
    assert(rs.getString("mycolumn") === "World!")

    stmt.close()
    conn.close()
  }
}
