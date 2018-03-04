package scorex.api.http.assets

import play.api.libs.json.{Format, Json}

case class GermTransferRequest(assetId: Option[String],
                           feeAssetId: Option[String],
                           amount: Long,
                           fee: Long,
                           sender: String,
                           attachment: Option[String],
                           recipient: String,
                           timestamp: Option[Long] = None)

object GermTransferRequest {
  implicit val transferFormat: Format[GermTransferRequest] = Json.format
}