package com.wavesplatform

import java.sql.{Connection, DriverManager, ResultSet}
import scorex.block.{Block, MicroBlock}

object PostgreDB {

  private final val con_str = "jdbc:postgresql://localhost:5432/BisChain?user=postgres&password=germ"
  private final val usePostgreSqlSetting = true

  private final val status_not_found = "Not_found"
  private final val status_create = "Create"
  private final val status_approve = "Approve"

  def usePostgresql(): Boolean = {usePostgreSqlSetting}

  def addToPostgreDB(block: Block): Unit = {
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)
    try {
      println("Postgres connector from addToPostgreDB from blockchain")

      block.transactionData.foreach(f = transaction => {
        val transJson = transaction.json.apply()
        val id = prepStr((transJson \ "id").get.toString)
        val attachment = prepStr((transJson \ "attachment").get.toString)
        val status = getTxStatus(id)

        if (status != status_not_found) {
          if (status == status_create) updatePostgreTx(conn, id, attachment, status_approve)
        } else createPostgreTx(conn, id, attachment, status_approve)
      })
    } finally {
      println("Postgres connector from addToPostgreDB from blockchain CLOSE")
      conn.close()
    }
  }

  def addToPostgreDB(microBlock: MicroBlock): Unit = {
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)
    try {
      println("Postgres connector from addToPostgreDB from client")

      microBlock.transactionData.foreach(transaction => {
        val transJson = transaction.json.apply()
        val id = prepStr((transJson \ "id").get.toString)
        val attachment = prepStr((transJson \ "attachment").get.toString)
        createPostgreTx(conn, id, attachment, status_create)
      })

    } finally {
      println("Postgres connector from addToPostgreDB from client CLOSE")
      conn.close()
    }
  }

   def readFromPostgreDB(): ResultSet = {
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)
    println("Postgres connector from readFromPostgreDB")
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      stm.executeQuery("SELECT * from public.transaction")

      /* пример обработки ответа
      while (rs.next) {
        println(rs.getString("tx_id") + " " + rs.getString("text"))
      }*/
    } finally {
      println("Postgres connector from readFromPostgreDB CLOSE")
      conn.close()
    }
  }

  def getTxStatus(tx_id: String): String = {
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(con_str)
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      var res = stm.executeQuery(s"SELECT Status FROM public.transaction WHERE tx_id = '$tx_id'")
      var status = status_not_found
      if(res.next){
        status = prepStr(res.getString("Status"))
      }
      println(s"getTxStatus tx_id=$tx_id status=$status")
      status

    } finally {
      conn.close()
    }
  }

  private def prepStr (str: String): String = {
     str.trim.replaceAll("^\"|\"$", "")
  }

  private def createPostgreTx (conn: Connection, id:String, attachment:String, status:String): Unit = {
    val prep = conn.prepareStatement("INSERT INTO public.transaction(tx_id, text, status) VALUES (?, ?, ?)")
    prep.setString(1, id)
    prep.setString(2, attachment)
    prep.setString(3, status)
    val execRes = prep.executeUpdate
    println(s"SQL INSERT DONE query($prep) res($execRes)")
  }

  private def updatePostgreTx (conn: Connection, id:String, attachment:String, status:String): Unit = {
    val prep = conn.prepareStatement("UPDATE public.transaction SET text=?, status=? WHERE tx_id=?")
    prep.setString(1, attachment)
    prep.setString(2, status)
    prep.setString(3, id)
    val execRes = prep.executeUpdate
    println(s"SQL UPDATE DONE query($prep) res($execRes)")
  }
}
