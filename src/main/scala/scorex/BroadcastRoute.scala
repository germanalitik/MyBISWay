package scorex

import com.wavesplatform.UtxPool
import com.wavesplatform.network._
import io.netty.channel.group.ChannelGroup
import scorex.api.http.ApiError
import scorex.transaction.{Transaction, ValidationError}

import scala.concurrent.Future

trait BroadcastRoute {
  def utx: UtxPool

  def allChannels: ChannelGroup

  import scala.concurrent.ExecutionContext.Implicits.global

  protected def doBroadcast(v: Either[ValidationError, Transaction]): Future[Either[ApiError, Transaction]] = Future {
    val r = for {
      tx <- v
      added <- utx.putIfNew(tx)
    } yield {
      if (added) allChannels.broadcastTx(tx, None)
      tx
    }

    r.left.map(ApiError.fromValidationError)
  }
}
