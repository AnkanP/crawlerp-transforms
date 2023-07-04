package kafka.connect.transforms.s3

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data.{Field, Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.transforms.util.{SchemaUtil, SimpleConfig}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.util
import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `list asScalaBuffer`}

abstract class InsertFields[R <: ConnectRecord[R]] extends Transformation[R] {

  private final val OVERVIEW_DOC = "Add Operation & timestamp field from header of source record to target record";
  final val logger = LoggerFactory.getLogger(classOf[InsertFields[R]])

  private var OP_fieldName: String = null
  private var TS_fieldName: String = null

  private var schemaUpdateCache: Cache[Schema, Schema] = null
  private val PURPOSE = "adding fields from headers"

  private object ConfigName {
    val OP_FIELD_NAME = "op.field.name";
    val TIMESTAMP_FIELD_NAME = "timestamp.field.name";
  }

  //1
  val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(ConfigName.OP_FIELD_NAME,ConfigDef.Type.STRING,ConfigDef.Importance.MEDIUM,"field name for operation")
    .define(ConfigName.TIMESTAMP_FIELD_NAME,ConfigDef.Type.STRING,ConfigDef.Importance.MEDIUM,"field name for timestamp")



  //2
  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)
    OP_fieldName = config.getString(ConfigName.OP_FIELD_NAME)
    TS_fieldName = config.getString(ConfigName.TIMESTAMP_FIELD_NAME)
    schemaUpdateCache = new SynchronizedCache[Schema,Schema](new LRUCache[Schema, Schema](16))
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

  def applyWithSchema(record: R): R = {

    val value = requireStruct(operatingValue(record), PURPOSE)

    /* for first record this is null */
    /* value.schema can be either the schema from kafka value or kafka key if it is a tombstone record */

    /* get schema from the cache using the key: keySchema */
    var updatedSchema = schemaUpdateCache.get(record.keySchema())

    if (updatedSchema == null ) {
      //If first record is a delete
      if(record.value() == null) {
        updatedSchema = getSchema(record: R) //Get schema from registry for tombstone record
        schemaUpdateCache.put(record.keySchema(), updatedSchema)
        logger.info("INSIDE FIRST IF")
      }
      else {
       // updatedSchema = value.schema()
        updatedSchema = makeOptionalSchema(record: R)
        schemaUpdateCache.put(record.keySchema(), updatedSchema)
        logger.info("INSIDE FIRST ELSE")
      }
    }
    // for tombstone record
    else if(record.value() == null) {
      updatedSchema = schemaUpdateCache.get(record.keySchema())
      logger.info("INSIDE SECOND ELSE IF")
    }
    else {
      //updatedSchema = value.schema()
      updatedSchema = makeOptionalSchema(record: R)
      schemaUpdateCache.put(record.keySchema(), updatedSchema)
      logger.info("INSIDE LAST ELSE")
    }

    /* add headers to schema */
    updatedSchema = buildHeaderSchema(updatedSchema)
    logger.info("CACHE:" + schemaUpdateCache.size())

    for(s <- updatedSchema.fields()){
      logger.info("FIELDS" + s.name() + " " +s.schema() + " isoptional: " + s.schema().isOptional)
    }

    /* Create a Struct object with schema*/
    var updatedValue = new Struct(updatedSchema)

    /* Add value for headers */
    //updatedValue = addHeaderFieldsToRecord(record, updatedValue)


    val valFieldMap: Map[String, Schema] = updatedSchema.fields().toList.zipWithIndex.map { case (k, v) => (k.name(), k.schema()) }.toMap
    val keyFieldMap: Map[String, Schema] = value.schema().fields().toList.zipWithIndex.map { case (k, v) => (k.name(), k.schema()) }.toMap
    var nonKeyFieldMap: Map[String, Schema] = Map.empty

    for ((k, v) <- valFieldMap) {
      if (!keyFieldMap.contains(k))
        nonKeyFieldMap += (k -> v.schema())
    }


    val headers = record.headers()
    val headerMap = headers.zipWithIndex.map{case (k,v) => (k.key(),k.value())}.toMap
    /* diffFieldSet size > 0 only in case of Tombstone record */

    for((k,v) <- nonKeyFieldMap) {
      if (!headerMap.contains(k)) updatedValue.put(k, null)
    }


    //populate fields
    for ((k,v) <- keyFieldMap) {
      updatedValue.put(k, value.get(k))
    }

    /* Add value for headers */
    updatedValue = addHeaderFieldsToRecord(record, updatedValue)

    newRecord(record, updatedSchema, updatedValue)
  }

  def addHeaderFieldsToRecord(record: R,updatedValue: Struct):Struct={

    val headers = record.headers()

    //set default values for operation & tc
    updatedValue.put(OP_fieldName, "defaultop")
    updatedValue.put(TS_fieldName, record.timestamp())

    for (header <- headers) {
      if (header.key() == OP_fieldName) {
        if (header.value().isInstanceOf[Array[Byte]]) {
        var operation = new String(header.value().asInstanceOf[Array[Byte]])
        logger.info("HEADER VALUE1:" + operation)
        updatedValue.put(OP_fieldName, operation)
      }
        else new RuntimeException("Header " + OP_fieldName + " not a String")
      }


      if (header.key() == TS_fieldName) {
        if (header.value().isInstanceOf[Array[Byte]]) {
          var tc = ByteBuffer.wrap(header.value().asInstanceOf[Array[Byte]]).getLong
          updatedValue.put(TS_fieldName, tc)
        }
        else new RuntimeException("Header " + TS_fieldName + " not a Long")
      }

    }
    updatedValue
  }


  def makeOptionalSchema(vRecord: R): Schema = {
    val builder: SchemaBuilder = SchemaUtil.copySchemaBasics(vRecord.valueSchema(), SchemaBuilder.struct)

    val valFieldMap: Map[String, Schema] = vRecord.valueSchema().fields().toList.zipWithIndex.map { case (k, v) => (k.name(), k.schema()) }.toMap
    val keyFieldMap: Map[String, Schema] = vRecord.keySchema().fields().toList.zipWithIndex.map { case (k, v) => (k.name(), k.schema()) }.toMap
    var nonKeyFieldMap: Map[String, Schema] = Map.empty

    //val dateSchema = SchemaBuilder.struct().field(k, org.apache.kafka.connect.data.Date.SCHEMA).optional().build()


    for ((k,v) <- valFieldMap) println(s"Valkey: $k, value: $v")
    for ((k,v) <- keyFieldMap) println(s"Keykey: $k, value: $v")


    for((k,v) <- valFieldMap) {
      if(! keyFieldMap.contains(k))
        nonKeyFieldMap += (k -> v.schema())
    }

    for ((k,v) <- nonKeyFieldMap) println(s"NonKey: $k, value: $v")

    for ((k,v) <- keyFieldMap) {
      logger.info("SCHEMA FIELD NAME: " + k+ " TYPE: " + v.schema().`type`())
      builder.field(k, v.schema)
    }

    for ((k,v) <- nonKeyFieldMap) {
        logger.info("type:" + v.schema()  + "schematype: " + v.schema().`type`())

        v.schema().toString match {
          case "Schema{STRING}" => builder.field(k, Schema.OPTIONAL_STRING_SCHEMA)
          case "Schema{INT64}" => builder.field(k, Schema.OPTIONAL_INT64_SCHEMA)
          case "Schema{INT32}" => builder.field(k, Schema.OPTIONAL_INT32_SCHEMA)
          case "Schema{INT16}" => builder.field(k, Schema.OPTIONAL_INT16_SCHEMA)
          case "Schema{INT8}" => builder.field(k, Schema.OPTIONAL_INT8_SCHEMA)
          case "Schema{FLOAT32}" => builder.field(k, Schema.OPTIONAL_FLOAT32_SCHEMA)
          case "Schema{FLOAT64}" => builder.field(k, Schema.OPTIONAL_FLOAT64_SCHEMA)
          case "Schema{BYTES}" => builder.field(k, Schema.OPTIONAL_BYTES_SCHEMA)
          case "Schema{BOOLEAN}" => builder.field(k, Schema.OPTIONAL_BOOLEAN_SCHEMA)
          case "Schema{org.apache.kafka.connect.data.Date:INT32}" =>  builder.field(k, SchemaBuilder.int32().name("org.apache.kafka.connect.data.Date").optional.version(1).build)
          case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" =>  {
            val scale = v.schema().parameters().get("scale").toInt
            builder.field(k, SchemaBuilder.bytes.name("org.apache.kafka.connect.data.Decimal").optional().parameter("scale", scale.toString).version(1).build())
          }

          case default  => builder.field(k,v.schema()).optional().build()
        }

    }

    for (s <- builder.fields()) {
      logger.info("INSIDE OPTIONAL FUNC FIELDS" + s.name() + " " + s.schema() + " isoptional: " + s.schema().isOptional + "defaultvalue: " + s.schema().defaultValue())
    }

 builder.build()
    builder.schema()
  }


  def buildHeaderSchema(schema: Schema): Schema={
    val builder: SchemaBuilder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)
    //val builder1: SchemaBuilder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)


    for (field: Field <- schema.fields.toList) {
      logger.info("SCHEMA FIELD NAME: " + field.name() + " TYPE: " + field.schema().`type`() )
      builder.field(field.name, field.schema)
    }

    builder.field(OP_fieldName, Schema.STRING_SCHEMA) //for operation
    builder.field(TS_fieldName, Schema.INT64_SCHEMA) //for timestamp
    //builder.schema()

    for (s <- builder.fields()) {
      logger.info("INSIDE HEADER FUNC FIELDS" + s.name() + " " + s.schema() + " isoptional: " + s.schema().isOptional + "default" + s.schema().defaultValue())
    }

    builder.build()
    builder.schema()
  }


  //def getSchema(paramSchema: Schema,topic: String): Schema = {
  def getSchema(vRecord: R): Schema = {

    /* Download schema from schema registry */
    val schemaRegistryClient = new CachedSchemaRegistryClient(System.getenv("SCHEMA_REGISTRY_URL"), 16)
    val valueSchema = schemaRegistryClient.getLatestSchemaMetadata(vRecord.topic() + "-value")
    //logger.info("SCHEMA:" + valueSchema.getSchema)

    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(valueSchema.getSchema)
    val builder: SchemaBuilder = new SchemaBuilder(Type.STRUCT).optional()


    /* exclude the key fields and set the schema for the non-key fields as per the data types */
    val keyFieldMap: Map[String, Schema] = vRecord.keySchema().fields().toList.zipWithIndex.map { case (k, v) => (k.name(), k.schema()) }.toMap

    for (field <- schema.getFields.toList) {

    logger.info("FIELD_NAME: " + field.name() + "TYPE: " + field.schema().getType  + ":" + field.schema().getLogicalType)
      if(!keyFieldMap.contains(field.name()))
        {
          field.schema().getType.toString match {
            case "INT" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
            case "LONG" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
            case "STRING" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            case "BYTES" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
            case "BOOLEAN" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
            case "FLOAT" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
            case "DOUBLE" => builder.field(field.name(), org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
          }
        }

      else
        builder.field(field.name(),keyFieldMap(field.name())).optional()
    }

    for (s <- builder.fields()) {
      logger.info("INSIDE GET SCHEMA FUNC FIELDS" + s.name() + " " + s.schema() + " isoptional: " + s.schema().isOptional + "default: " + s.schema().defaultValue())
    }

    builder.build()
    builder.schema()
  }

  // static members
  protected def operatingSchema(record: R): Schema
  protected def operatingValue(record: R): Object
  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R

}

object InsertFields {
  //static implementation

  class Key[R <: ConnectRecord[R]] extends InsertFields[R] {
    override protected def operatingSchema(record: R): Schema = record.keySchema

    override protected def operatingValue(record: R): Object = record.key

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedSchema, updatedValue, record.valueSchema, record.value, record.timestamp)
  }
  class Value[R <: ConnectRecord[R]] extends InsertFields[R] {
    override protected def operatingSchema(record: R): Schema = if (record.valueSchema != null) record.valueSchema() else record.keySchema()

    override protected def operatingValue(record: R): Object = if(record.value != null) record.value() else record.key()

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, updatedSchema, updatedValue, record.timestamp)
  }
}
