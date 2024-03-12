package kafka.connect.transforms.kafka

import io.confluent.connect.avro.{AvroData, AvroDataConfig}
import kafka.connect.transforms.s3.InsertFields
import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{ConnectSchema, Date, Decimal, Schema, SchemaBuilder, Struct, Time}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.transforms.util.{SchemaUtil, SimpleConfig}
import org.joda.time.DateTimeUtils
import org.slf4j.LoggerFactory

import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.avro

abstract class OverrideSchema[R <: ConnectRecord[R]] extends Transformation[R] {

  private final val OVERVIEW_DOC = "Add Operation & timestamp field from header of source record to target record";
  final val logger = LoggerFactory.getLogger(classOf[InsertFields[R]])

  private var CUSTOM_AVRO_SCHEMA_SUPPORT: String = null
  private var CUSTOM_AVRO_METADATA: String = null

  private var schemaUpdateCache: Cache[Schema, Schema] = null
  private val PURPOSE = "adding fields from headers"

  private object ConfigName {
    val CUSTOM_AVRO_SCHEMA_SUPPORT = "custom.enhanced.avro.schema.support";
    val CUSTOM_AVRO_METADATA = "custom.connect.meta.data";
   }

  // static members
  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Object

  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R

  val CONFIG_DEF: ConfigDef = new ConfigDef()
  .define(ConfigName.CUSTOM_AVRO_SCHEMA_SUPPORT, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "avro schema support")
  .define(ConfigName.CUSTOM_AVRO_METADATA, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "connect avro metadata")


  //2
  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)
    CUSTOM_AVRO_SCHEMA_SUPPORT = config.getString(ConfigName.CUSTOM_AVRO_SCHEMA_SUPPORT)
    CUSTOM_AVRO_METADATA = config.getString(ConfigName.CUSTOM_AVRO_METADATA)
    schemaUpdateCache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](16))
  }

  //3
  override def apply(record: R): R = {

    /* For a tombstone record this returns with key schema */
    /* If Record with non-schema throw exception */
    if (operatingSchema(record) == null)
      new RuntimeException("Schemaless record found. Aborting!!")

    applyWithSchema(record)
  }

  override def config(): ConfigDef = CONFIG_DEF

  override def close(): Unit = schemaUpdateCache = null


  private def applyWithSchema(record: R): R = {

    val value = requireStruct(operatingValue(record), PURPOSE)

    //var updatedSchema = schemaUpdateCache.get(record.valueSchema())
    //var updatedSchema = schemaUpdateCache.get(value.schema())

    logger.info("CACHE:" + schemaUpdateCache.size())


    for (s <- value.schema().fields().asScala) {
      logger.info("FIELDS: " + s.name() + " " + s.schema() + " isoptional: " + s.schema().isOptional)
    }


    // Convert connect schema to avro schema
    /** Create a avro schema & convert to connect schema */

    val taskProperties: Properties = new Properties()
    taskProperties.setProperty("enhanced.avro.schema.support", CUSTOM_AVRO_SCHEMA_SUPPORT)
    taskProperties.setProperty("connect.meta.data", CUSTOM_AVRO_METADATA)
    val avroData = new AvroData(new AvroDataConfig(taskProperties))
    // convert connect schema to avro schema
    val avroValSchema: org.apache.avro.Schema = avroData.fromConnectSchema(value.schema())

Date.

    //val newSchemaBuilder: org.apache.avro.Schema =  avro.SchemaBuilder.

    for (schema <- avroValSchema.getFields.asScala){
      logger.info("AVRO SCHEMA: " + schema.name() + "  " + schema.schema().toString(true) + " ")
      schema.getProp()

    }




    // convert avro schema back to connect schema



    //Override schema
    val updatedSchema = makeSchema(record)
    for (s <- updatedSchema.fields().asScala) {
        logger.info("OVERRIDE FIELDS: " + s.name() + " " + s.schema() + " isoptional : " + s.schema().isOptional)
    }

    /* Create a Struct object with schema*/
    var updatedValue = new Struct(updatedSchema)

    for (x <- updatedSchema.fields().asScala) {

      try {
        updatedValue.put(x.name(), value.get(x.name()))
      } catch {
        case e: Exception => {
          print("ORIGINAL Value " + value.get(x.name()))
          logger.error(e.getMessage)
          updatedValue.put(x.name(), System.currentTimeMillis() * 1000)
        }
        }

    }
    //logging-

    for(s <- updatedSchema.schema().fields().asScala){
      logger.info("DISPLAY FIELDS: " + s.name() + " " + s.schema() + " value : " + updatedValue.get(s.name()))
    }

    newRecord(record, updatedSchema, updatedValue)
  }


  def makeSchema(record: R): Schema = {
    val builder: SchemaBuilder = SchemaUtil.copySchemaBasics(record.valueSchema(), SchemaBuilder.struct)





    for (rec <- record.valueSchema().fields().asScala) {
      //logger.info("OVERRIDE FIELDS: " + rec.name() + " " + rec.schema() + " isoptional: " + rec.schema().isOptional)
      rec.schema().toString match {
        case "Schema{STRING}" => builder.field(rec.name().toLowerCase, Schema.STRING_SCHEMA)
        case "Schema{INT64}" => builder.field(rec.name().toLowerCase, Schema.INT64_SCHEMA)
        case "Schema{INT32}" => builder.field(rec.name().toLowerCase, Schema.INT32_SCHEMA)
        case "Schema{INT16}" => builder.field(rec.name().toLowerCase, Schema.INT16_SCHEMA)
        case "Schema{INT8}" => builder.field(rec.name().toLowerCase, Schema.INT8_SCHEMA)
        case "Schema{FLOAT32}" => builder.field(rec.name().toLowerCase, Schema.FLOAT32_SCHEMA)
        case "Schema{FLOAT64}" => builder.field(rec.name().toLowerCase, Schema.FLOAT64_SCHEMA)
        case "Schema{BYTES}" => builder.field(rec.name().toLowerCase, Schema.BYTES_SCHEMA)
        case "Schema{BOOLEAN}" => builder.field(rec.name().toLowerCase, Schema.BOOLEAN_SCHEMA)
        case "Schema{org.apache.kafka.connect.data.Time:INT32}" => builder.field(rec.name().toLowerCase, SchemaBuilder.int32().name(Time.LOGICAL_NAME)
          .version(1)
          .build)
        case "Schema{org.apache.kafka.connect.data.Date:INT32}" => builder.field(rec.name().toLowerCase, SchemaBuilder.int32().name(Date.LOGICAL_NAME)
          .version(1)
          .build)
        case "Schema{org.apache.kafka.connect.data.Timestamp:INT64}" => {


          //logger.info("TIMESTAMP LOGICAL TYPE: " + rec.schema().parameters().get("logicalType").toString)
          val paramMap =  Map ("logicalType" -> "timestamp-micros", "key" -> "0")

          val adjSchema = new ConnectSchema(Schema.INT64_SCHEMA.`type`(),
              false,
            null,
            "testtype",
            1,
            "custom timestamp",
            paramMap.asJava,
            null,
            Schema.INT64_SCHEMA,
            Schema.INT64_SCHEMA
          )
          builder.field(rec.name().toLowerCase, adjSchema)
          //builder.field(rec.name().toLowerCase, connectSchema)
        }
        case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" => {
          val scale = rec.schema().parameters().get("scale").toInt
          builder.field(rec.name().toLowerCase, SchemaBuilder.bytes.name(Decimal.LOGICAL_NAME)
            .parameter("scale", scale.toString)
            .parameter("precision", "10")
            .version(1)
            .build())
        }

        case default => builder.field(rec.name(), rec.schema()).optional().build()
      }

      rec.schema()
    }

    builder.build()
    builder.schema()
  }

}

object OverrideSchema {
  //static implementation

  class Key[R <: ConnectRecord[R]] extends OverrideSchema[R] {
    override protected def operatingSchema(record: R): Schema = record.keySchema

    override protected def operatingValue(record: R): Object = record.key

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedSchema, updatedValue, record.valueSchema, record.value, record.timestamp)
  }

  class Value[R <: ConnectRecord[R]] extends OverrideSchema[R] {
    override protected def operatingSchema(record: R): Schema = if (record.valueSchema != null) record.valueSchema() else record.keySchema()

    override protected def operatingValue(record: R): Object = if (record.value != null) record.value() else record.key()

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, updatedSchema, updatedValue, record.timestamp)
  }

}