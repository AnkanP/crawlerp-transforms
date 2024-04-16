package kafka.connect.transforms.kafka

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Date, Decimal, Schema, SchemaBuilder, Struct, Time, Timestamp}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.slf4j.LoggerFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.time.Instant
import java.util
import scala.collection.JavaConverters._
import scala.math.BigDecimal

abstract class StagingToDataTransformer[R <: ConnectRecord[R]] extends Transformation[R] {

  private final val OVERVIEW_DOC = "staging to data"
  final val logger = LoggerFactory.getLogger(classOf[StagingToDataTransformer[R]])

   private var VALUE_NODE: String = ""
   private var KEY_NODE: String = ""

  private var schemaUpdateCache: Cache[Schema, Schema] = _

  private object ConfigName {
    val VALUE_NODE = "value.json.node"
    val KEY_NODE   = "key.json.node"
  }

  val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(ConfigName.VALUE_NODE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, OVERVIEW_DOC)
    .define(ConfigName.KEY_NODE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, OVERVIEW_DOC)

  private case class FlattenMapReturnType(builder: SchemaBuilder, map: util.LinkedHashMap[String, Any])
  private case class SchemaAndValue(schema: Schema, map: util.LinkedHashMap[String, Any])

  override def apply(r: R): R = {
    applySchemaless(r)
  }

  private def applySchemaless(r: R): R = {

    val input: String = r.value().toString
    val mapper = new ObjectMapper()
    val jsonNode: JsonNode = parseJsonBytes(mapper, input.getBytes(StandardCharsets.UTF_8))
    val schemaValueVal = schemaAndVal(mapper,VALUE_NODE, jsonNode)
    val schemaKeyVal = schemaAndVal(mapper, KEY_NODE, jsonNode)

    // Value Schema
    val valueSchema = schemaValueVal.schema
    var updatedValSchema = schemaUpdateCache.get(valueSchema)
    if (updatedValSchema == null) {
      schemaUpdateCache.put(valueSchema, updatedValSchema)
      updatedValSchema = valueSchema
    }

    //Key Schema
    val keySchema = schemaKeyVal.schema
    var updatedKeySchema = schemaUpdateCache.get(keySchema)
    if (updatedKeySchema == null) {
      schemaUpdateCache.put(keySchema, updatedKeySchema)
      updatedKeySchema = keySchema
    }

    val structVal: Struct = new Struct(updatedValSchema)
    val structKey: Struct = new Struct(updatedKeySchema)

    if(logger.isDebugEnabled()){
      for (s <- updatedValSchema.schema().fields().asScala) {
        logger.debug("updated schema: " + s.name() + " " + s.schema())
      }

      //for (entry <- schemaValueVal.map.entrySet().asScala) {
      //  logger.info("VALUE NODE:- " + "key: " + entry.getKey + " val: " + entry.getValue.toString)
      //  structVal.put(entry.getKey, entry.getValue)
      //}
//
      //for (entry <- schemaKeyVal.map.entrySet.asScala) {
      //  logger.info("KEY NODE:- " + "key: " + entry.getKey + " val: " + entry.getValue.toString)
      //  structKey.put(entry.getKey, entry.getValue)
      //}


      val valueMap =  schemaValueVal.map
      for (s <- updatedValSchema.schema().fields().asScala) {

        logger.debug("updated value fields : " + s.name() + " " + s.schema() + " value : " + valueMap.get(s.name()) + " type: " + valueMap.get(s.name()).getClass)
        s.schema().toString match {
          case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" => structVal.put(s.name(), java.math.BigDecimal.valueOf(valueMap.get(s.name()).asInstanceOf[java.lang.Double])  )
          case _ => structVal.put(s.name(), valueMap.get(s.name()) )
        }

      }

      val keyMap = schemaKeyVal.map
      for (s <- updatedKeySchema.schema().fields().asScala) {

        logger.debug("updated key fields : " + s.name() + " " + s.schema() + " value : " + keyMap.get(s.name()))
        s.schema().toString match {
          case "Schema{org.apache.kafka.connect.data.Decimal:BYTES}" => structKey.put(s.name(), keyMap.get(s.name()).toString.getBytes )
          case _ => structKey.put(s.name(), keyMap.get(s.name()))
        }
      }

    }

    newRecord(r, updatedKeySchema,structKey ,updatedValSchema, structVal)

  }


  override def config(): ConfigDef = CONFIG_DEF

  override def close(): Unit = schemaUpdateCache = null

  override def configure(map: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, map)
    VALUE_NODE = config.getString(ConfigName.VALUE_NODE)
    KEY_NODE = config.getString(ConfigName.KEY_NODE)
    schemaUpdateCache = new SynchronizedCache[Schema,Schema](new LRUCache[Schema, Schema](16))
  }


  private def parseJsonBytes(mapper: ObjectMapper, input: Array[Byte] ): JsonNode = {
    val inputStream: ByteArrayInputStream = new ByteArrayInputStream(input)
    val jsonParser: JsonParser = mapper.getFactory.createParser(inputStream)
    val jsonNode: JsonNode = mapper.readValue(jsonParser, classOf[JsonNode])
    jsonNode

  }

  private def schemaAndVal( mapper: ObjectMapper, jsonNodePath: String, jsonNode: JsonNode): SchemaAndValue = {
    val builder: SchemaBuilder = SchemaBuilder.struct()
    builder.name("dummy")
    builder.doc("dummy")
    builder.version(1)

    val flatten = flattenAndMap(mapper,"dummy", jsonNode.get(jsonNodePath), new util.LinkedHashMap[String, Any](), builder)
    SchemaAndValue(flatten.builder.build(), flatten.map)
  }

  private def flattenAndMap(mapper: ObjectMapper ,key: String, node: JsonNode, map: util.LinkedHashMap[String, Any], builder: SchemaBuilder): FlattenMapReturnType = {
    node.getNodeType match {
      case JsonNodeType.OBJECT =>
        val it: java.util.Iterator[java.util.Map.Entry[String, JsonNode]] = node.fields()
        while (it.hasNext) {
          val entry = it.next
          flattenAndMap(mapper, entry.getKey, entry.getValue, map, builder)
        }
      case JsonNodeType.NULL =>
        map.put(key, null)
      case JsonNodeType.BOOLEAN =>
        builder.field(key, Schema.BOOLEAN_SCHEMA)
        map.put(key, node.booleanValue())
      case JsonNodeType.NUMBER =>
        if (node.isIntegralNumber) {
          map.put(key, node.longValue())
          builder.field(key, Schema.INT64_SCHEMA)
        }
        else {
          map.put(key, node.doubleValue())
          builder.field(key, Schema.FLOAT64_SCHEMA)
        }
      case JsonNodeType.STRING =>
        map.put(key, node.textValue())
        builder.field(key, Schema.STRING_SCHEMA)

      case JsonNodeType.ARRAY =>
        //without case class
        val fieldsList: java.util.List[java.util.Map[String,Any]] = mapper.readValue[java.util.List[java.util.Map[String, Any]]](node.toPrettyString, classOf[java.util.List[java.util.Map[String, Any]]])
        fieldsList.asScala.foreach(field => {
          field.get("type").toString.toUpperCase() match {
            case x if x.startsWith("CHAR") =>
              map.put(field.get("name").toString, field.get("value").toString)
              builder.field(field.get("name").toString, Schema.STRING_SCHEMA)

            case x if x.startsWith("INT") =>
              map.put(field.get("name").toString, field.get("value").asInstanceOf[Int])
              builder.field(field.get("name").toString, Schema.INT32_SCHEMA)

            case x if x.startsWith("LONG") =>
              map.put(field.get("name").toString, field.get("value").asInstanceOf[Long])
              builder.field(field.get("name").toString, Schema.INT64_SCHEMA)

            case x if x.startsWith("BYTE") =>
              map.put(field.get("name").toString, field.get("value").asInstanceOf[Byte])
              builder.field(field.get("name").toString, Schema.BYTES_SCHEMA)

            case x if x.startsWith("FLOAT") =>
              map.put(field.get("name").toString, field.get("value").asInstanceOf[Float])
              builder.field(field.get("name").toString, Schema.FLOAT32_SCHEMA)


            case x if x.startsWith("DOUBLE") =>
              map.put(field.get("name").toString, field.get("value").asInstanceOf[Double])
              builder.field(field.get("name").toString, Schema.FLOAT64_SCHEMA)



            case x if x.startsWith("DATE") =>
              val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val parsedDate = dateFormat.parse(field.get("value").toString)
              map.put(field.get("name").toString, parsedDate)
              builder.field(field.get("name").toString, Date.SCHEMA)

            case x if x.startsWith("TIMESTAMP") =>
              /** All Datetime conversions using Java8 Time API*/
              /** The Below pattern assumes the timestamp is in UTC */
              //val datetimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-ddTHH:mm:ss.SSSZ")
              //val parsedTimestamp: LocalDateTime = LocalDateTime.parse(field.get("value").toString, datetimeFormat)
              //val timestamp =  java.sql.Timestamp.valueOf(parsedTimestamp)
              val instant: Instant = Instant.parse(field.get("value").toString)
              val timestamp = java.sql.Timestamp.from(instant)
              map.put(field.get("name").toString, timestamp)
              builder.field(field.get("name").toString, Timestamp.SCHEMA)

            case x if x.startsWith("TIME") =>
              val timeFormat = new SimpleDateFormat("HH:mm:ss")
              val parsedTime = timeFormat.parse(field.get("value").toString)
              map.put(field.get("name").toString, parsedTime)
              builder.field(field.get("name").toString, Time.SCHEMA)

            case x if x.startsWith("NUMERIC") =>
              //val doubleVal = field.get("value").asInstanceOf[Double]
              val decimalVal =  java.math.BigDecimal.valueOf(field.get("value").asInstanceOf[java.lang.Double])
              map.put(field.get("name").toString, field.get("value"))

              // parse
              val scale = 1
              val precision = 15
              val PRECISION_FIELD = "connect.decimal.precision"
              val fieldBuilder = Decimal.builder(scale)
              fieldBuilder.parameter(PRECISION_FIELD,Integer.toString(precision))
              builder.field(field.get("name").toString,fieldBuilder.build())
              //builder.field(field.get("name").toString, Schema.FLOAT64_SCHEMA)



            case _ =>
              println("do nothing")
          }

        })

      case _ => throw new RuntimeException("Unsupported Json Node Type Found!! " + node.getNodeType.toString)

    }

    FlattenMapReturnType(builder, map)
  }

  // static members
  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Object

  protected def newRecord(record: R, updatedKeySchema: Schema, updatedKey: Object, updatedValueSchema: Schema, updatedValue: Object): R

}


object StagingToDataTransformer {
  //static implementation

  class Key[R <: ConnectRecord[R]] extends StagingToDataTransformer[R] {
    override protected def operatingSchema(record: R): Schema = record.keySchema

    override protected def operatingValue(record: R): Object = record.key

    override protected def newRecord(record: R, updatedKeySchema: Schema, updatedKey: Object, updatedValueSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedKeySchema, updatedKey, record.valueSchema, record.value, record.timestamp)
  }

  class Value[R <: ConnectRecord[R]] extends StagingToDataTransformer[R] {
    override protected def operatingSchema(record: R): Schema = record.valueSchema()

    override protected def operatingValue(record: R): Object = record.value()

    override protected def newRecord(record: R, updatedKeySchema: Schema, updatedKey: Object, updatedValueSchema: Schema, updatedValue: Object): R = record.newRecord(record.topic, record.kafkaPartition, updatedKeySchema, updatedKey, updatedValueSchema, updatedValue, record.timestamp)
  }

}