package kafka.connect.transforms.kafka

import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.connect.transforms.util.Requirements.{requireStruct, requireStructOrNull}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import java.util

 abstract class Raw2DataTransformer[R <: ConnectRecord[R]] extends Transformation[R]{

   final val logger = LoggerFactory.getLogger(classOf[Transformation[R]])

  private var OP_fieldName: String = _
  private var TS_fieldName: String = _
   private var schemaUpdateCache: Cache[Schema, Schema] = null
   private val PURPOSE = "adding fields from headers"

  private object ConfigName {
    val OP_FIELD_NAME = "op.field.name"
    val TIMESTAMP_FIELD_NAME = "raw2data.timestamp.field.name"
  }

  val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(ConfigName.OP_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "operation header" )
    .define(ConfigName.TIMESTAMP_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "raw2data timestamp header")


   def makeUpdatedSchema(schema: Schema): Schema = {

     import org.apache.kafka.connect.data.SchemaBuilder
     import org.apache.kafka.connect.transforms.util.SchemaUtil
     val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)


     for (field <- schema.fields) {

       logger.info("FIELDS:" + field.name() + " " +field.schema() )

       field.name() match {
         case "magic" => builder.field(field.name, field.schema)
         case "version" => builder.field(field.name, field.schema)
         case "after" => {

           for(structField <- field.schema().fields()){
             builder.field(structField.name, structField.schema)
             logger.info("STRUCT FIELDS:" + structField.name() + " " +structField.schema() )
           }

         }

         case default =>
       }

     }
     builder.build()

   }

   def applyWithSchema(record: R): R = {
     val value = requireStruct(operatingValue(record), PURPOSE)
     var updatedSchema = schemaUpdateCache.get(value.schema)
     if (updatedSchema == null) {
       updatedSchema = makeUpdatedSchema(value.schema)
       schemaUpdateCache.put(value.schema, updatedSchema)
     }

     val updatedValue = new Struct(updatedSchema)

     logger.info("RECORD STRUCT:" + value.toString)
     logger.info("RECORD:" + record)

     for (field <- updatedValue.schema.fields) {

       val after = requireStruct(value.getStruct("after"),PURPOSE)
       val before = requireStructOrNull(value.getStruct("before"),PURPOSE)

       field.name() match {
         case "magic" => updatedValue.put(field.name, value.get(field.name()))
         case "version" => updatedValue.put(field.name, value.get(field.name()))

         case default => {
           logger.info("VALUE FIELD:" + field.name() + " " + after.get(field.name())  + " "  )
           updatedValue.put(field.name, after.get(field.name()))
         }
       }

       }

      newRecord(record, updatedSchema, updatedValue)

   }

   override def apply(record: R): R = {

     if (operatingSchema(record) == null)
       new RuntimeException("Schemaless record found. Aborting!!")

    if (operatingValue(record) == null) record  // Need to revisit if the IF condition is required
    else  applyWithSchema(record)
  }

  override def config(): ConfigDef = CONFIG_DEF

  override def close(): Unit = {

  }

  override def configure(map: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, map)
    OP_fieldName = config.getString(ConfigName.OP_FIELD_NAME)
    TS_fieldName = config.getString(ConfigName.TIMESTAMP_FIELD_NAME)

    schemaUpdateCache = new SynchronizedCache[Schema,Schema](new LRUCache[Schema, Schema](16))
  }


  // static members
  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Object

  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R
}

object Raw2DataTransformer {

  class Key[R <: ConnectRecord[R]] extends Raw2DataTransformer[R] {

    override protected def operatingSchema(record: R): Schema = record.keySchema

    override protected def operatingValue(record: R): Object = record.key

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedSchema, updatedValue, record.valueSchema, record.value, record.timestamp)
  }


  class Value[R <: ConnectRecord[R]] extends Raw2DataTransformer[R] {
    override protected def operatingSchema(record: R): Schema = record.valueSchema()

    override protected def operatingValue(record: R): Object =  record.value()

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, updatedSchema, updatedValue, record.timestamp)
  }

}


