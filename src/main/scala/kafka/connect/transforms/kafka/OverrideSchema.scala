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
      logger.info("updated fields: " + s.name() + "value: " + value.get(s.name()))
    }


    val updatedValue = new Struct(updatedSchema)
    for (x <- updatedValue.schema().fields().asScala) {
      if (VARCHAR_TO_VARBINARY_FIELDS.contains(x.name())) {
        x.schema().toString match {
          case "Schema{BYTES}" => updatedValue.put(x.name(), value.get(x.name()).toString.getBytes(StandardCharsets.UTF_8))
          case default => updatedValue.put(x.name(), value.get(x.name()))
        }
      } else {
        updatedValue.put(x.name(), value.get(x.name()))
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
          case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" => if (!schema.schema().isOptional) builder.field(schema.name(), Schema.FLOAT64_SCHEMA)
          else builder.field(schema.name(), Schema.OPTIONAL_FLOAT64_SCHEMA)
          case default => builder.field(schema.name(), schema.schema())
        }
      } else {
        builder.field(schema.name(), schema.schema())
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
