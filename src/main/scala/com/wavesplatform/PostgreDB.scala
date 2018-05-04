package com.wavesplatform

import java.sql.{DriverManager, ResultSet}

import play.api.libs.json.{JsValue, Json}
import scorex.block.Block

object PostgreDB {

  private val con_str = "jdbc:postgresql://localhost:5432/BisChain?user=postgres&password=germ"
  private val usePostgreSql = true

  def usePostgresql(): Boolean = {return usePostgreSql}

  def addToPostgreDB(block: Block): Unit = {
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)
    try {
      println("Postgres connector from addToPostgreDB")

      val transactions = (Json.parse(block.json().toString()) \ "transactions").get
      val ids = transactions \\ "id"
      val attachments = transactions \\ "attachment"

      for (i <- 0 until ids.length) {
        val prep = conn.prepareStatement("INSERT INTO public.transaction(tx_id, text) VALUES (?, ?)")
        prep.setString(1, ids(i).toString().trim.replaceAll("^\"|\"$", ""))
        prep.setString(2, attachments(i).toString().trim.replaceAll("^\"|\"$", ""))
        prep.executeUpdate
        println("SQL INSERT DONE " + prep)
      }
    } finally {
      println("Postgres connector from addToPostgreDB CLOSE")
      conn.close()
    }
  }

   def readFromPostgreDB(): ResultSet = {
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)
    println("Postgres connector from readFromPostgreDB")
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      return stm.executeQuery("SELECT * from public.transaction")

      /* пример обработки ответа
      while (rs.next) {
        println(rs.getString("tx_id") + " " + rs.getString("text"))
      }*/
    } finally {
      println("Postgres connector from readFromPostgreDB CLOSE")
      conn.close()
    }
  }
}
