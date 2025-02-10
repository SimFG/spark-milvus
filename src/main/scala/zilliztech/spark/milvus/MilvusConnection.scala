package zilliztech.spark.milvus

import io.milvus.client.MilvusServiceClient
import io.milvus.param.ConnectParam

import org.slf4j.LoggerFactory
import scala.collection.mutable

case class MilvusConnection (
                              client: MilvusServiceClient) {
}

object MilvusConnection {
  private val cache = new mutable.HashMap[String, MilvusServiceClient]
  private val log = LoggerFactory.getLogger(getClass)

  def acquire(milvusOptions: MilvusOptions): MilvusServiceClient = {
    lazy val connectParam = if (milvusOptions.uri.isEmpty) {
      ConnectParam.newBuilder
        .withHost(milvusOptions.host)
        .withPort(milvusOptions.port)
        .withAuthorization(milvusOptions.userName, milvusOptions.password)
        .withDatabaseName(milvusOptions.databaseName)
        .build
    } else {
      // zilliz cloud
      ConnectParam.newBuilder
        .withUri(milvusOptions.uri)
        .withToken(milvusOptions.token)
        .build
    }

    log.info(s"connection info ${milvusOptions.host}.${milvusOptions.port}, status: ${milvusOptions.uri}")
    new MilvusServiceClient(connectParam)
//    cache.getOrElseUpdate(milvusOptions.toString, new MilvusServiceClient(connectParam))
  }
}