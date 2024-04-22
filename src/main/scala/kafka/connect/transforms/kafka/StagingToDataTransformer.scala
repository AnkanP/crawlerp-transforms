package kafka.connect.transforms.kafka

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Date, Schema, SchemaBuilder, Struct, Time, Timestamp}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.slf4j.LoggerFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.time.Instant
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`


abstract class StagingToDataTransformer[R <: ConnectRecord[R]] extends Transformation[R] {

   private final val OVERVIEW_DOC = "staging to data"
   final val logger = LoggerFactory.getLogger(classOf[StagingToDataTransformer[R]])

   private var VALUE_NODE: String = ""
   private var KEY_NODE: String = ""

  private var schemaUpdateCache: Cache[Schema, Schema] = _
  private val mapper = new ObjectMapper()

  private object ConfigName {
    val VALUE_NODE = "value.json.node"
    val KEY_NODE   = "key.json.node"
  }

  val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(ConfigName.VALUE_NODE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, OVERVIEW_DOC)
    .define(ConfigName.KEY_NODE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, OVERVIEW_DOC)

  private case class FlattenMapReturnType(builder: SchemaBuilder, map: scala.collection.mutable.Map[String, Any])
  private case class SchemaAndValue(schema: Schema, map: scala.collection.mutable.Map[String, Any])

  override def apply(r: R): R = {
    applySchemaless(r)
  }

  private def applySchemaless(r: R): R = {

    val input_data: String = r.value().toString


    //val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
    //mapper.registerModule(DefaultScalaModule)
    //mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    // To be used for scala versions > 2.10
    //val newMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
    //case class NestedField(name: String, `type`: String, value: String)

    case class FlattenMapReturnType(builder: SchemaBuilder, map: scala.collection.mutable.Map[String, Any])
    case class SchemaAndValue(schema: Schema, map: scala.collection.mutable.Map[String, Any])

    val jsonNode: JsonNode = parseJsonBytes(input_data.getBytes(StandardCharsets.UTF_8))

    val schemaValueVal = schemaAndVal(VALUE_NODE, jsonNode)
    val schemaKeyVal = schemaAndVal(KEY_NODE, jsonNode)

    val structVal: Struct = new Struct(schemaValueVal.schema)
    val structKey: Struct = new Struct(schemaKeyVal.schema)


    for (entry <- schemaValueVal.map.entrySet().asScala) {
      structVal.put(entry.getKey, entry.getValue)
    }

    for (entry <- schemaKeyVal.map.entrySet.asScala) {
      structKey.put(entry.getKey, entry.getValue)
    }


    print("VALUE: " + structVal.toString)
    println("KEY: " + structKey.toString)
    for (field <- schemaValueVal.schema.fields().asScala) {
      print("VALUE SCHEMA:" + field.name() + " - " + field.schema().toString)
    }
    for (field <- schemaKeyVal.schema.fields().asScala) {
      print("KEY SCHEMA:" + field.name() + " - " + field.schema().toString)
    }

    newRecord(r, schemaKeyVal.schema,structKey ,schemaValueVal.schema, structVal)

  }


  private def parseJsonBytes(input: Array[Byte]): JsonNode = {
    val inputStream: ByteArrayInputStream = new ByteArrayInputStream(input)
    val jsonParser: JsonParser = mapper.getFactory.createParser(inputStream)
    val jsonNode: JsonNode = mapper.readValue(jsonParser, classOf[JsonNode])
    jsonNode

  }

  def parseJsonString(input: String): JsonNode = {
    val jsonNode: JsonNode = mapper.readValue(input, classOf[JsonNode])
    jsonNode

  }


  private def schemaAndVal(jsonNodepath: String, jsonNode: JsonNode): SchemaAndValue = {
    val builder: SchemaBuilder = SchemaBuilder.struct()
    builder.name("staging2data")
    builder.doc("staging2data")
    builder.version(1)
    val flatten = flattenAndMap("staging2data", jsonNode.get(jsonNodepath), scala.collection.mutable.Map[String, Any](), builder)
    SchemaAndValue(flatten.builder.build(), flatten.map)
  }


  private def flattenAndMap(key: String, node: JsonNode, map: scala.collection.mutable.Map[String, Any], builder: SchemaBuilder): FlattenMapReturnType = {

    node.getNodeType match {
      case JsonNodeType.OBJECT => {
        val it: java.util.Iterator[java.util.Map.Entry[String, JsonNode]] = node.fields()
        while (it.hasNext) {
          val entry = it.next
          flattenAndMap(entry.getKey, entry.getValue, map, builder)
        }

      }
      case JsonNodeType.NULL => {
        map.put(key, null)
      }
      case JsonNodeType.BOOLEAN => {
        builder.field(key, Schema.BOOLEAN_SCHEMA)
        map.put(key, node.booleanValue())
      }
      case JsonNodeType.NUMBER => {
        if (node.isIntegralNumber) {
          map.put(key, node.longValue())
          builder.field(key, Schema.INT64_SCHEMA)
        }
        else {
          map.put(key, node.doubleValue())
          builder.field(key, Schema.FLOAT64_SCHEMA)
        }
      }
      case JsonNodeType.STRING => {
        map.put(key, node.textValue())
        builder.field(key, Schema.STRING_SCHEMA)

      }

      case JsonNodeType.ARRAY => {
        //without case class
        //Need to use java structures here as scala object mapper module is throwing error
        val fieldsList: util.List[util.Map[String, Any]] = mapper.readValue[util.List[util.Map[String, Any]]](node.toPrettyString, classOf[util.List[util.Map[String, Any]]])
        fieldsList.asScala.foreach(field => {
          //val elemMap: java.util.Map[String, Any] = field.asInstanceOf[java.util.Map[String, Any]]
          val elemMap = field.asScala
          val z = elemMap.get("value") match {
            case Some(s) if (s) == null => ("null")
            case Some(s) if (s) == "NULL" => ("abcdef")
            case Some("NULL") => ("xyz")
            case Some(s) => (s)
            case None => ("key not found")
          }
          print(elemMap.get("type").toString.toUpperCase() + ":" + elemMap.get("name").toString.toUpperCase() + z.toString.toUpperCase())
          print("\n")

          elemMap.get("type").getOrElse().toString.toUpperCase() match {

            case x if x.startsWith("BOOLEAN") =>
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[String])
              builder.field(field.get("name").toString, Schema.OPTIONAL_BOOLEAN_SCHEMA)

            case x if x.startsWith("CHAR") => {
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[String])
              builder.field(elemMap.get("name").getOrElse().toString, Schema.OPTIONAL_STRING_SCHEMA)
            }

            case x if x.startsWith("INT") =>
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[Int])
              builder.field(field.get("name").toString, Schema.OPTIONAL_INT32_SCHEMA)

            case x if x.startsWith("LONG") =>
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[Long])
              builder.field(field.get("name").toString, Schema.OPTIONAL_INT64_SCHEMA)

            case x if x.startsWith("BYTE") =>
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[Byte])
              builder.field(field.get("name").toString, Schema.OPTIONAL_BYTES_SCHEMA)

            case x if x.startsWith("FLOAT") =>
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[Float])
              builder.field(field.get("name").toString, Schema.OPTIONAL_FLOAT32_SCHEMA)


            case x if x.startsWith("DOUBLE") =>
              map.put(elemMap.get("name").getOrElse().toString, nullCheck(elemMap.get("value")).asInstanceOf[Double])
              builder.field(field.get("name").toString, Schema.OPTIONAL_FLOAT64_SCHEMA)

            case x if x.startsWith("DATE") =>
              val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val value = nullCheck(elemMap.get("value"))
              val parsedDate = if (value != null) dateFormat.parse(value.asInstanceOf[String]) else null
              map.put(elemMap.get("name").getOrElse().toString, parsedDate)
              builder.field(field.get("name").toString, SchemaBuilder.int32().name(Date.SCHEMA.name()).optional.version(1).build)
            //builder.field(field.get("name").toString, Date.SCHEMA)

            case x if x.startsWith("TIMESTAMP") =>

              /** All Datetime conversions using Java8 Time API */
              /** The Below pattern assumes the timestamp is in UTC */
              //val datetimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-ddTHH:mm:ss.SSSZ")
              //val parsedTimestamp: LocalDateTime = LocalDateTime.parse(field.get("value").toString, datetimeFormat)
              //val timestamp =  java.sql.Timestamp.valueOf(parsedTimestamp)
              val value = nullCheck(elemMap.get("value"))
              val timestamp = if (value != null) java.sql.Timestamp.from(Instant.parse(value.asInstanceOf[String])) else null
              map.put(field.get("name").toString, timestamp)

              builder.field(field.get("name").toString, SchemaBuilder.int64().name(Timestamp.SCHEMA.name()).optional.version(1).build)
            //builder.field(field.get("name").toString, Timestamp.SCHEMA)

           case x if x.startsWith("TIME") =>
             val timeFormat = new SimpleDateFormat("HH:mm:ss")
             val value = nullCheck(elemMap.get("value"))
             val parsedTime = if (value != null) timeFormat.parse(value.asInstanceOf[String]) else null
             map.put(field.get("name").toString, parsedTime)
//
             builder.field(field.get("name").toString, SchemaBuilder.int32().name(Time.SCHEMA.name()).optional.version(1).build)
           //builder.field(field.get("name").toString, Time.SCHEMA)

            case default => {
              //println("do nothing")
            }
          }

        })


      }

      case default => throw new RuntimeException("Unsupported Json Node Type Found!! " + node.getNodeType.toString)

    }

    FlattenMapReturnType(builder, map)
  }


  private def nullCheck(o: Option[Any]): Any = {

    val v = o match {
      case Some(s) if (s) == null => null
      case Some(s) if (s) == "NULL" => null
      case Some("NULL") => ("xyz")
      case Some(s) => (s)
      case None => ("key not found")
    }
    v
  }


  override def config(): ConfigDef = CONFIG_DEF

  override def close(): Unit = schemaUpdateCache = null

  override def configure(map: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, map)
    VALUE_NODE = config.getString(ConfigName.VALUE_NODE)
    KEY_NODE = config.getString(ConfigName.KEY_NODE)
    schemaUpdateCache = new SynchronizedCache[Schema,Schema](new LRUCache[Schema, Schema](16))
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