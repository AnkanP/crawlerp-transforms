package kafka.connect.transforms.s3

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner
import org.apache.kafka.connect.sink.SinkRecord
import org.joda.time.DateTimeZone
import org.joda.time.Instant.now
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import java.util
import java.util.Locale


class TimeAndPartitionBasedPartitioner extends TimeBasedPartitioner {

 private final val logger = LoggerFactory.getLogger(classOf[TimeAndPartitionBasedPartitioner])


  var vtimeZone:DateTimeZone = null
  var vpartitionDurationMs: Long = 0
  var vPathFormat = ""

  var directoryPrefix: Object = _
  override def init(partitionDurationMs: Long, pathFormat: String, locale: Locale, timeZone: DateTimeZone, config: util.Map[String, Object]): Unit =
    {

      vtimeZone = timeZone
      vpartitionDurationMs = partitionDurationMs
      vPathFormat = pathFormat
      directoryPrefix = config.get("directory.prefix")



      super.init(partitionDurationMs, pathFormat, locale, timeZone, config)

    }

 override def encodePartition(sinkRecord: SinkRecord, nowInMillis: Long): String = {
   var timestampPartitions = ""

   vPathFormat match {
     case "YYYYMMdd"|"YYYY-MM-dd" => {
       timestampPartitions = super.encodePartition(sinkRecord, nowInMillis)
       logger.info("time partition" + timestampPartitions)
       logger.info("millis: " + nowInMillis.toString)
     }
     case "YYYYMMddHHmm" => {
       val adjustedTimestamp = vtimeZone.convertUTCToLocal(nowInMillis)
       val partitionedTime = adjustedTimestamp / vpartitionDurationMs * vpartitionDurationMs
       vtimeZone.convertLocalToUTC(partitionedTime, false)
       timestampPartitions = DateTimeFormat.forPattern(vPathFormat).print(partitionedTime)
     }
     case _ => new RuntimeException("PathFormat not supported!!")
   }

   val partition_no = sinkRecord.kafkaPartition().toString
   val partition =    timestampPartitions + this.delim + partition_no
   logger.info("Encoded partition : {}", partition)
   partition

 }

 override def encodePartition(sinkRecord: SinkRecord): String = {
   var timestampPartitions = ""

   vPathFormat match {
     case "YYYYMMdd"|"YYYY-MM-dd" => timestampPartitions = super.encodePartition(sinkRecord )
     case "YYYYMMDDHHmm" => {
       val adjustedTimestamp = vtimeZone.convertUTCToLocal(now().getMillis)
       val partitionedTime = adjustedTimestamp / vpartitionDurationMs * vpartitionDurationMs
       vtimeZone.convertLocalToUTC(partitionedTime, false)
       timestampPartitions = DateTimeFormat.forPattern(vPathFormat).print(partitionedTime)
     }
     case _ => new RuntimeException("PathFormat not supported!!")
   }

   val partition_no = sinkRecord.kafkaPartition().toString
   val partition =  timestampPartitions + this.delim + partition_no
  logger.info("Encoded partition : {}", partition)
   partition
 }


  // override this  method to generate a custom top level directory
  override def generatePartitionedPath(topic: String, encodedPartition: String): String =
    //super.generatePartitionedPath(topic, encodedPartition)
    directoryPrefix.toString + encodedPartition

}




