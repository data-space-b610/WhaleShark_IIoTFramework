package ksb.csle.ingestion.runner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

public class PhoenixTest {
  public static void main(String args[]) throws Exception {
    Connection conn;
    Properties prop = new Properties();
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    conn = DriverManager.getConnection("jdbc:phoenix:[zookeeper]");
    System.out.println("got connection");
    ResultSet rst = conn.createStatement().executeQuery("select * from stock_symbol");
    while (rst.next()) {
      System.out.println(rst.getString(1) + " " + rst.getString(2));
    }
    System.out.println(conn.createStatement().executeUpdate("delete from stock_symbol"));
    conn.commit();
    rst = conn.createStatement().executeQuery("select * from stock_symbol");
    while (rst.next()) {
      System.out.println(rst.getString(1) + " " + rst.getString(2));
    }
    System.out.println(conn.createStatement()
        .executeUpdate("upsert into stock_symbol values('IBM','International Business Machines')"));
    conn.commit();
  }
}