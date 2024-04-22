package kafka.connect.transforms.kafka

import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.{SchemaUtil, SimpleConfig}
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.header.{ConnectHeaders, Headers}
import org.apache.kafka.connect.transforms.util.Requirements.{requireStruct, requireStructOrNull}
import org.slf4j.LoggerFactory
import org.apache.kafka.connect.data.SchemaBuilder

import scala.collection.JavaConversions._
import java.util

/**
 * <R extends TableRecord<R>>

It means a class of type R, that implements the interface TableRecord<R>

TableRecord<R> means that the interface is bound to the same type R.

An example would be a class like:

public class Bla implements TableRecord<Bla>
I admit this seems a bit strange, but Java generics don't really differentiate between extends and implements, which leads to some confusion.
 *
 * @tparam R
 */
abstract class RetroCompatibleRaw2DataTransformer[R <: ConnectRecord[R]] extends Transformation[R]{

  final val logger = LoggerFactory.getLogger(classOf[Transformation[R]])
  private var schemaUpdateCache: Cache[Schema, Schema] = null
  private val PURPOSE = "adding fields from headers"
  var headers = new ConnectHeaders
  private var operation = ""
  var topicCommitTimestamp = System.currentTimeMillis()


  val CONFIG_DEF: ConfigDef = new ConfigDef()

  private def makeUpdatedKeySchema(schema: Schema): Schema = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)
    builder.field("magic", Schema.INT32_SCHEMA)
    builder.field("version", Schema.INT32_SCHEMA)
    for (field <- schema.fields) {
      builder.field(field.name(),field.schema())
    }
    builder.build()

  }


  private def makeUpdatedSchema(schema: Schema): Schema = {

    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    builder.field("magic", Schema.INT32_SCHEMA)
    builder.field("version", Schema.INT32_SCHEMA)
    for (field <- schema.fields) {

      logger.info("FIELDS:" + field.name() + " " +field.schema()    )
      field.name() match {
        //case "magic" => builder.field(field.name, field.schema)
        //case "version" => builder.field(field.name, field.schema)
        case "after" => {

          for(structField <- field.schema().fields()){
            builder.field(structField.name, structField.schema)
            logger.info("STRUCT FIELDS:" + structField.name() + " " +structField.schema() )
          }

        }

        case _ =>
      }

    }
    builder.build()

  }



  def applyWithSchema(record: R): R = {
    val value = requireStruct(operatingValue(record), PURPOSE)
    val key = requireStruct(operatingKey(record),PURPOSE)

    var updatedSchema = schemaUpdateCache.get(value.schema)
    var updatedKeySchema = schemaUpdateCache.get(key.schema)



    /** Generate schema if not in cache. Update cache with updated schema */
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema())
      schemaUpdateCache.put(value.schema, updatedSchema)
    }

    if (updatedKeySchema == null) {
      updatedKeySchema = makeUpdatedKeySchema(key.schema())
      schemaUpdateCache.put(key.schema(), updatedKeySchema)
    }


    var updatedValue = new Struct(updatedSchema)

    val updatedKey = new Struct(updatedKeySchema)

    for (field <- updatedKey.schema.fields) {

      field.name() match {
        case "magic" => updatedKey.put(field.name, 1)
        case "version" => updatedKey.put(field.name, 1)
        case _ => {
          updatedKey.put(field.name(), key.get(field.name()))
        }
      }
    }


    logger.info("RECORD:" + record)
    logger.info("RECORD STRUCT:" + value.toString)

    val after = requireStructOrNull(value.getStruct("after"), PURPOSE)
    val before = requireStructOrNull(value.getStruct("before"), PURPOSE)
    val dbCommitTime = requireStructOrNull(value.getStruct("header"),PURPOSE).get("timestamp").asInstanceOf[Long]

    for (field <- updatedValue.schema.fields) {

      field.name() match {
        case "magic" => updatedValue.put(field.name, 1)
        case "version" => updatedValue.put(field.name, 1)

        case _ => {


          if (before != null && after != null) {
            operation = "U"
            logger.info("VALUE FIELD:" + field.name() + " " + after.get(field.name())  + " "  )
            updatedValue.put(field.name, after.get(field.name()))
          }

          else if (after != null) {
            operation = "I"
            logger.info("VALUE FIELD:" + field.name() + " " + after.get(field.name())  + " "  )
            updatedValue.put(field.name, after.get(field.name()))

          }
          else if (before != null) {
            operation = "D"
            updatedValue = null // generate tombstone markers
            updatedSchema = null
          }
          else {
            logger.warn("Both before & after images found to be null. Key: " + record.key())
          }
        }
      }

    }

    topicCommitTimestamp = System.currentTimeMillis()
    headers = new ConnectHeaders  //Reinitialize for every record
    headers.add("tc", BigInt(dbCommitTime).toByteArray, Schema.BYTES_SCHEMA) //Database commit timestamp
    headers.add("ta", BigInt(record.timestamp()).toByteArray, Schema.BYTES_SCHEMA) //Abinitio kafka commit timestamp
    headers.add("tr", BigInt(topicCommitTimestamp).toByteArray, Schema.BYTES_SCHEMA) // Datatopic message creation timestamp
    headers.add("o", operation.getBytes, Schema.BYTES_SCHEMA)  // operation


    //print headers
    for(header <- headers){
      logger.info("HEADER: " + header)
    }

  print("VAL RECORD" + updatedValue)
    print("KEY RECORD" + updatedKey)
    newRecord(record, updatedSchema, updatedValue,updatedKeySchema,updatedKey)

  }

  override def apply(record: R): R = {

    if (operatingSchema(record) == null)
      new RuntimeException("Schemaless record found. Aborting!!")

    //if (operatingValue(record) == null) record // Need to revisit if the IF condition is required
    //else
    applyWithSchema(record)


  }

  override def config(): ConfigDef = CONFIG_DEF

  override def close(): Unit = {

  }

  override def configure(map: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, map)

    schemaUpdateCache = new SynchronizedCache[Schema,Schema](new LRUCache[Schema, Schema](16))
  }


  // static members
  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Object

  protected def operatingKey(record: R): Object

  protected def operatingKeySchema(record: R): Schema

  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object,updatedKeySchema: Schema, updatedKey: Object): R
}

object RetroCompatibleRaw2DataTransformer {

class Key[R <: ConnectRecord[R]] extends RetroCompatibleRaw2DataTransformer[R] {
  override protected def operatingSchema(record: R): Schema = record.keySchema()

  override protected def operatingValue(record: R): Object = record.key()

  override protected def operatingKey(record: R): Object = record.key

  override protected def operatingKeySchema(record: R): Schema = record.keySchema

  //override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object), updatedKeySchema: Schema: R = record.newRecord(record.topic, record.kafkaPartition, makeUpdatedKeySchema(record.keySchema), record.key, updatedSchema, updatedValue, topicCommitTimestamp,headers)

  override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object, updatedKeySchema: Schema, updatedKey: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedKeySchema, updatedKey, updatedSchema, updatedValue, topicCommitTimestamp, headers)
}




  class Value[R <: ConnectRecord[R]] extends RetroCompatibleRaw2DataTransformer[R] {
    override protected def operatingSchema(record: R): Schema = record.valueSchema()

    override protected def operatingValue(record: R): Object =  record.value()

    override protected def operatingKey(record: R): Object = record.key()

    override protected def operatingKeySchema(record: R): Schema = record.keySchema

    //override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object), updatedKeySchema: Schema: R = record.newRecord(record.topic, record.kafkaPartition, makeUpdatedKeySchema(record.keySchema), record.key, updatedSchema, updatedValue, topicCommitTimestamp,headers)

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object, updatedKeySchema: Schema, updatedKey: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedKeySchema, updatedKey, updatedSchema, updatedValue, topicCommitTimestamp,headers)
  }

}


