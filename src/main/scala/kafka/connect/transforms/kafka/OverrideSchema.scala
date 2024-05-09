package kafka.connect.transforms.kafka


import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.{ConfigDef, ConfigException}
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{ConnectSchema, Date, Decimal, Schema, SchemaBuilder, Struct, Time, Timestamp}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.transforms.util.{SchemaUtil, SimpleConfig}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import java.nio.charset.StandardCharsets
import java.time.Instant


abstract class OverrideSchema[R <: ConnectRecord[R]] extends Transformation[R] {

  private final val OVERVIEW_DOC = "schema overrides";
  final val logger = LoggerFactory.getLogger(classOf[OverrideSchema[R]])

  private var VARCHAR_TO_VARBINARY_FIELDS: util.List[String] = _

  private var schemaUpdateCache: Cache[Schema, Schema] = null
  private val PURPOSE = "schema overrides"

  private object ConfigName {
    val VARCHAR_TO_VARBINARY_FIELDS = "varchar.to.varbinary.fields";
  }

  // static members
  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Object

  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R

  val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(ConfigName.VARCHAR_TO_VARBINARY_FIELDS,
      ConfigDef.Type.LIST,
      ConfigDef.NO_DEFAULT_VALUE,
      new ConfigDef.Validator() {
        def ensureValid(name: String, valueObject: Object): Unit = {
          val value: util.List[String] = valueObject.asInstanceOf[util.List[String]]
          if (value == null || value.isEmpty) {
            throw new ConfigException("Must specify at least one field to cast.");
          }
        }
      },
      ConfigDef.Importance.MEDIUM,
      OVERVIEW_DOC
    )

  //2
  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)
    VARCHAR_TO_VARBINARY_FIELDS = config.getList(ConfigName.VARCHAR_TO_VARBINARY_FIELDS)
    schemaUpdateCache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](16))
  }

  //3
  override def apply(record: R): R = {
    /* If Record with non-schema throw exception */
    if (operatingSchema(record) == null)
      new RuntimeException("Schemaless record found. Aborting!!")

    applyWithSchema(record)
  }

  override def config(): ConfigDef = CONFIG_DEF

  override def close(): Unit = schemaUpdateCache = null


  private def applyWithSchema(record: R): R = {

    val value = requireStruct(operatingValue(record), PURPOSE)
    val valueSchema = operatingSchema(record)
    //Override schema
    var updatedSchema = schemaUpdateCache.get(valueSchema)
    if (updatedSchema == null) {
      updatedSchema = convertToLowerCase(valueSchema)
      if (!VARCHAR_TO_VARBINARY_FIELDS.isEmpty) {
        //convert list elemets to lowercase
        VARCHAR_TO_VARBINARY_FIELDS.replaceAll(_.toLowerCase)
        updatedSchema = schemaConversion(valueSchema, VARCHAR_TO_VARBINARY_FIELDS)
      }
      schemaUpdateCache.put(valueSchema, updatedSchema)
    }



    for (s <- updatedSchema.schema().fields().asScala) {
      logger.info("updated schema: " + s.name() + " " + s.schema() )
    }

    for (s <- value.schema().fields().asScala) {
      logger.info("updated fields: " + s.name() + " value: " + value.get(s.name()) + " type=" + value.get(s.name()).getClass)
    }


    val updatedValue = new Struct(updatedSchema)
    for (x <- updatedValue.schema().fields().asScala) {
      if (VARCHAR_TO_VARBINARY_FIELDS.contains(x.name())) {
        x.schema().toString match {
          case "Schema{BYTES}" => updatedValue.put(x.name(), value.get(x.name()).toString.getBytes(StandardCharsets.UTF_8))
          case _ => println("EXCEPTION TO BE RAISED!!. Expected field value of type varchar or string")
        }
      } else {
        x.schema().toString match {
          case "Schema{BYTES}" => updatedValue.put(x.name(), value.get(x.name()).toString.getBytes(StandardCharsets.UTF_8))
          case "Schema{FLOAT64}" => updatedValue.put(x.name(), value.get(x.name()).asInstanceOf[java.math.BigDecimal].doubleValue())
          //case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" => updatedValue.put(x.name(), java.math.BigDecimal.valueOf(value.get(x.name()).asInstanceOf[java.lang.Double]))
          //case "Schema{org.apache.kafka.connect.data.Time:INT32}" => updatedValue.put(x.name(), value.get(x.name()).asInstanceOf[java.sql.Time].getTime * 1000L)
          case "Schema{org.apache.kafka.connect.data.Timestamp:INT64}" =>{
            //val instant: Instant = Instant.parse(value.get(x.name()).asInstanceOf[java.sql.Timestamp].toString)
            val instant: Instant = value.get(x.name()).asInstanceOf[java.sql.Timestamp].toInstant
            val microseconds = instant.toEpochMilli * 1000L
            //val timestamp: java.sql.Timestamp = java.sql.Timestamp.from(instant)
            val timestamp: java.sql.Timestamp = new java.sql.Timestamp(microseconds)
            updatedValue.put(x.name(), timestamp)
          }
          case _ => updatedValue.put(x.name(), value.get(x.name()))
        }


      }
    }


    for (s <- updatedSchema.schema().fields().asScala) {
      logger.info("updated fields after: " + s.name() + " " + s.schema() + " value : " + updatedValue.get(s.name()))
    }


    newRecord(record, updatedSchema, updatedValue)
  }


  private def convertToLowerCase(schema: Schema): Schema = {
    val builder: SchemaBuilder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    for (schema <- schema.fields().asScala) {
      builder.field(schema.name().toLowerCase, schema.schema())
    }
    builder.build()
  }

  private def schemaConversion(schema: Schema, fieldList: util.List[String]) = {
    val builder: SchemaBuilder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)
    for (schema <- schema.fields().asScala) {

      if (fieldList.contains(schema.name())) {
        schema.schema().toString match {
          case "Schema{STRING}" => {
            if (!schema.schema().isOptional) builder.field(schema.name(), Schema.BYTES_SCHEMA)
            else builder.field(schema.name(), Schema.OPTIONAL_BYTES_SCHEMA)
          }
          case _ => println("EXCEPTION TO BE RAISED!!. Expected field of type varchar or string")
        }
      } else {
        schema.schema().toString match {
          case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" => {

            val scale: Int = schema.schema().parameters().get("scale").toInt
            logger.info("FIELD=" + schema.schema().name()  + "_PARMETERS=" + schema.schema().parameters().toString + "_SCHEMA=" + schema.schema().toString)
            val PRECISION_FIELD = "connect.decimal.precision"
            val precision: Int = schema.schema().parameters().get(PRECISION_FIELD).toInt + 1

            if(scale == 0)
              builder.field(schema.name(), Schema.FLOAT64_SCHEMA)
            else {
              //val PRECISION_FIELD = "connect.decimal.precision"
              val fieldBuilder = Decimal.builder(scale)
              fieldBuilder.parameter(PRECISION_FIELD, Integer.toString(precision))
              fieldBuilder.parameter(PRECISION_FIELD, Integer.toString(scale))
              builder.field(schema.name(), fieldBuilder.build())

              //builder.field(schema.name(), SchemaBuilder.bytes.name("org.apache.kafka.connect.data.Decimal").optional().parameter("scale", scale.toString).parameter("precision", precision.toString).version(1).build())
            }


          }
          //case "Schema{org.apache.kafka.connect.data.Timestamp:INT64}" => {
          //  logger.info("FIELD=" + schema.schema().name()  + "_PARMETERS=" + schema.schema().parameters().toString + "_SCHEMA=" + schema.schema().toString)
          //  builder.field(schema.name(), schema.schema())

          //}

          case _ => builder.field(schema.name(), schema.schema())
        }
      }
    }
    builder.build()
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
