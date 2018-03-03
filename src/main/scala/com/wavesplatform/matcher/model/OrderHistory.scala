package com.wavesplatform.matcher.model

import cats.implicits._
import com.wavesplatform.db.{OrderIdsCodec, PortfolioCodec, SubStorage}
import com.wavesplatform.matcher.MatcherSettings
import com.wavesplatform.matcher.model.Events.{Event, OrderAdded, OrderCanceled, OrderExecuted}
import com.wavesplatform.matcher.model.LimitOrder.{Filled, OrderStatus}
import com.wavesplatform.matcher.model.OrderHistory.OrderHistoryOrdering
import com.wavesplatform.state2._
import org.iq80.leveldb.DB
import play.api.libs.json.Json
import scorex.transaction.AssetAcc
import scorex.transaction.assets.exchange.{AssetPair, Order}
import scorex.utils.ScorexLogging

trait OrderHistory {
  def orderAccepted(event: OrderAdded): Unit

  def orderExecuted(event: OrderExecuted): Unit

  def orderCanceled(event: OrderCanceled)

  def orderStatus(id: String): OrderStatus

  def orderInfo(id: String): OrderInfo

  def openVolume(assetAcc: AssetAcc): Long

  def openVolumes(address: String): Option[Map[String, Long]]

  def ordersByAddress(address: String): Set[String]

  def fetchOrderHistoryByPair(assetPair: AssetPair, address: String): Seq[(String, OrderInfo, Option[Order])]

  def fetchAllOrderHistory(address: String): Seq[(String, OrderInfo, Option[Order])]

  def deleteOrder(address: String, orderId: String): Boolean

  def order(id: String): Option[Order]

  def openPortfolio(address: String): OpenPortfolio
}

object OrderHistory {

  import OrderInfo.orderStatusOrdering

  object OrderHistoryOrdering extends Ordering[(String, OrderInfo, Option[Order])] {
    def orderBy(oh: (String, OrderInfo, Option[Order])): (OrderStatus, Long) = (oh._2.status, -oh._3.map(_.timestamp).getOrElse(0L))

    override def compare(first: (String, OrderInfo, Option[Order]), second: (String, OrderInfo, Option[Order])): Int = {
      implicitly[Ordering[(OrderStatus, Long)]].compare(orderBy(first), orderBy(second))
    }
  }

}

case class OrderHistoryImpl(db: DB, settings: MatcherSettings) extends SubStorage(db: DB, "matcher") with OrderHistory with ScorexLogging {

  private val OrdersPrefix = "orders".getBytes(Charset)
  private val OrdersInfoPrefix = "infos".getBytes(Charset)
  private val AddressToOrdersPrefix = "addr-orders".getBytes(Charset)
  private val AddressPortfolioPrefix = "portfolios".getBytes(Charset)

  def savePairAddress(address: String, orderId: String): Unit = {
    get(makeKey(AddressToOrdersPrefix, address)) match {
      case Some(valueBytes) =>
        val prev = OrderIdsCodec.decode(valueBytes).explicitGet().value
        var r = prev
        if (prev.length >= settings.maxOrdersPerRequest) {
          val (p1, p2) = prev.span(!orderStatus(_).isFinal)
          r = if (p2.isEmpty) p1 else p1 ++ p2.tail
        }
        put(makeKey(AddressToOrdersPrefix, address), OrderIdsCodec.encode(r :+ orderId), None)
      case _ =>
        put(makeKey(AddressToOrdersPrefix, address), OrderIdsCodec.encode(Array(orderId)), None)
    }
  }

  def saveOrderInfo(event: Event): Unit = {
    Events.createOrderInfo(event).foreach { case (orderId, oi) =>
      put(makeKey(OrdersInfoPrefix, orderId), orderInfo(orderId).combine(oi).jsonStr.getBytes(Charset), None)
      log.debug(s"Changed OrderInfo for: $orderId -> " + orderInfo(orderId))
    }
  }

  def openPortfolio(address: String): OpenPortfolio = {
    get(makeKey(AddressPortfolioPrefix, address)).map(PortfolioCodec.decode).map(_.explicitGet().value)
      .map(OpenPortfolio.apply).getOrElse(OpenPortfolio.empty)
  }

  def saveOpenPortfolio(event: Event): Unit = {
    Events.createOpenPortfolio(event).foreach { case (address, op) =>
      val key = makeKey(AddressPortfolioPrefix, address)
      val prev = get(key).map(PortfolioCodec.decode).map(_.explicitGet().value).map(OpenPortfolio.apply).getOrElse(OpenPortfolio.empty)
      val updatedPortfolios = prev.combine(op)
      put(key, PortfolioCodec.encode(updatedPortfolios.orders), None)
      log.debug(s"Changed OpenPortfolio for: $address -> " + updatedPortfolios.toString)
    }
  }

  def saveOrder(order: Order): Unit = {
    val key = makeKey(OrdersPrefix, order.idStr())
    if (get(key).isEmpty)
      put(key, order.jsonStr.getBytes(Charset), None)
  }

  def deleteFromOrders(orderId: String): Unit = {
    delete(makeKey(OrdersPrefix, orderId), None)
  }

  override def orderAccepted(event: OrderAdded): Unit = {
    val lo = event.order
    saveOrder(lo.order)
    saveOrderInfo(event)
    saveOpenPortfolio(event)
    savePairAddress(lo.order.senderPublicKey.address, lo.order.idStr())
  }

  override def orderExecuted(event: OrderExecuted): Unit = {
    saveOrder(event.submitted.order)
    savePairAddress(event.submitted.order.senderPublicKey.address, event.submitted.order.idStr())
    saveOrderInfo(event)
    saveOpenPortfolio(OrderAdded(event.submittedExecuted))
    saveOpenPortfolio(event)
  }

  override def orderCanceled(event: OrderCanceled): Unit = {
    saveOrderInfo(event)
    saveOpenPortfolio(event)
  }

  override def orderInfo(id: String): OrderInfo =
    get(makeKey(OrdersInfoPrefix, id)).map(Json.parse).flatMap(_.validate[OrderInfo].asOpt).getOrElse(OrderInfo.empty)

  override def order(id: String): Option[Order] = {
    import scorex.transaction.assets.exchange.OrderJson.orderFormat
    get(makeKey(OrdersPrefix, id)).map(b => new String(b, Charset)).map(Json.parse).flatMap(_.validate[Order].asOpt)
  }

  override def orderStatus(id: String): OrderStatus = {
    orderInfo(id).status
  }

  override def openVolume(assetAcc: AssetAcc): Long = {
    val asset = assetAcc.assetId.map(_.base58).getOrElse(AssetPair.WavesName)
    get(makeKey(AddressPortfolioPrefix, assetAcc.account.address)).map(PortfolioCodec.decode).map(_.explicitGet().value)
      .flatMap(_.get(asset)).map(math.max(0L, _)).getOrElse(0L)
  }

  override def openVolumes(address: String): Option[Map[String, Long]] = {
    get(makeKey(AddressPortfolioPrefix, address)).map(PortfolioCodec.decode).map(_.explicitGet().value)
  }

  override def ordersByAddress(address: String): Set[String] = {
    get(makeKey(AddressToOrdersPrefix, address)).map(OrderIdsCodec.decode).map(_.explicitGet().value)
      .map(_.toSet).getOrElse(Set())
  }

  override def deleteOrder(address: String, orderId: String): Boolean = {
    orderStatus(orderId) match {
      case Filled | LimitOrder.Cancelled(_) =>
        deleteFromOrders(orderId)
        deleteFromOrdersInfo(orderId)
        deleteFromAddress(address, orderId)
        true
      case _ =>
        false
    }
  }

  override def fetchOrderHistoryByPair(assetPair: AssetPair, address: String): Seq[(String, OrderInfo, Option[Order])] = {
    ordersByAddress(address)
      .toSeq
      .map(id => (id, orderInfo(id), order(id)))
      .filter(_._3.exists(_.assetPair == assetPair))
      .sorted(OrderHistoryOrdering)
      .take(settings.maxOrdersPerRequest)
  }

  override def fetchAllOrderHistory(address: String): Seq[(String, OrderInfo, Option[Order])] = {
    import OrderInfo.orderStatusOrdering
    ordersByAddress(address)
      .toSeq
      .map(id => (id, orderInfo(id)))
      .sortBy(_._2.status)
      .take(settings.maxOrdersPerRequest)
      .map(p => (p._1, p._2, order(p._1)))
      .sorted(OrderHistoryOrdering)
  }

  private def deleteFromOrdersInfo(orderId: String): Unit = delete(makeKey(OrdersInfoPrefix, orderId), None)

  private def deleteFromAddress(address: String, orderId: String): Unit = {
    val key = makeKey(AddressToOrdersPrefix, address)
    get(key) match {
      case Some(bytes) =>
        val prev = OrderIdsCodec.decode(bytes).explicitGet().value
        if (prev.contains(orderId)) put(key, OrderIdsCodec.encode(prev.filterNot(_ == orderId)), None)
      case _ =>
    }
  }
}
