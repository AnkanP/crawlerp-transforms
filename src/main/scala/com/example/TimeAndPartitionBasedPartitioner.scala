package com.example


import io.confluent.connect.storage.partitioner.TimeBasedPartitioner
import org.apache.kafka.connect.sink.SinkRecord
import org.joda.time.Instant.now
import org.joda.time.{DateTimeZone, Instant}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import java.util
import java.util.Locale


class TimeAndPartitionBasedPartitioner extends TimeBasedPartitioner {

 private final val logger = LoggerFactory.getLogger(classOf[TimeAndPartitionBasedPartitioner])


  var vtimeZone:DateTimeZone = null
  var vpartitionDurationMs: Long = 0
  var vPathFormat = ""
  override def init(partitionDurationMs: Long, pathFormat: String, locale: Locale, timeZone: DateTimeZone, config: util.Map[String, Object]): Unit =
    {

      vtimeZone = timeZone
      vpartitionDurationMs = partitionDurationMs
      vPathFormat = pathFormat

      super.init(partitionDurationMs, pathFormat, locale, timeZone, config)

    }

 override def encodePartition(sinkRecord: SinkRecord, nowInMillis: Long): String = {
   var timestampPartitions = ""

   vPathFormat match {
     case "YYYYMMDD" => timestampPartitions = super.encodePartition(sinkRecord, nowInMillis)
     case "YYYYMMDDHHmm" => {
       val adjustedTimestamp = vtimeZone.convertUTCToLocal(nowInMillis)
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

 override def encodePartition(sinkRecord: SinkRecord): String = {
   var timestampPartitions = ""

   vPathFormat match {
     case "YYYYMMDD" => timestampPartitions = super.encodePartition(sinkRecord )
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


}




