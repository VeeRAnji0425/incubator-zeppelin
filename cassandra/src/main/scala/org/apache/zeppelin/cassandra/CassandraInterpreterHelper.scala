package org.apache.zeppelin.cassandra

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core.DataType.Name._
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.{LoggingRetryPolicy, FallthroughRetryPolicy, DowngradingConsistencyRetryPolicy, Policies}
import org.apache.zeppelin.cassandra.CassandraInterpreter._
import org.apache.zeppelin.display.Input.ParamOption
import org.apache.zeppelin.interpreter.InterpreterResult.Code
import org.apache.zeppelin.interpreter.{InterpreterException, InterpreterResult, InterpreterContext}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable


case class CassandraQueryOptions(consistency: Option[ConsistencyLevel],
                                 serialConsistency:Option[ConsistencyLevel],
                                 timestamp: Option[Long],
                                 retryPolicy: Option[RetryPolicy])

object CassandraInterpreterHelper {
  
  val QUERY_OPTIONS_PREFIX: String = "@"
  val COMMENT_PREFIX: String = "#"
  val COMMA = ";"
  val BEGIN = "BEGIN"
  val BATCH_BEGIN = "BEGIN\\s+(?:UNLOGGED|COUNTER)?\\s*BATCH"
  val BATCH_APPLY = "APPLY BATCH"
  val CHOICES_SEPARATOR : String = """\|"""
  val VARIABLE_PATTERN = """\{\{[^}]+\}\}""".r
  val SIMPLE_VARIABLE_DEFINITION_PATTERN = """\{\{([^=]+)=([^=]+)\}\}""".r
  val MULTIPLE_CHOICES_VARIABLE_DEFINITION_PATTERN = """\{\{([^=]+)=((?:[^=]+\|)+[^|]+)\}\}""".r

  val CONSISTENCY_PREFIX = QUERY_OPTIONS_PREFIX + "consistency"
  val CONSISTENCY_LEVEL_PATTERN = ConsistencyLevel.values().toList
    .map(_.name()).filter(!_.contains("SERIAL"))
    .mkString(QUERY_OPTIONS_PREFIX + """consistency\s*=\s*(""","|",")").r

  val SERIAL_CONSISTENCY_PREFIX = QUERY_OPTIONS_PREFIX + "serialConsistency"
  val SERIAL_CONSISTENCY_LEVEL_PATTERN = ConsistencyLevel.values().toList
    .map(_.name()).filter(_.contains("SERIAL"))
    .mkString(QUERY_OPTIONS_PREFIX + """serialConsistency\s*=\s*(""","|",")").r

  val TIMESTAMP_PREFIX = QUERY_OPTIONS_PREFIX + "timestamp"
  val TIMESTAMP_PATTERN = (QUERY_OPTIONS_PREFIX + """timestamp\s*=\s*([0-9]+)""").r

  val RETRY_POLICIES_PREFIX = QUERY_OPTIONS_PREFIX + "retryPolicy"
  val RETRY_POLICIES_PATTERN = List(DEFAULT_POLICY,DOWNGRADING_CONSISTENCY_RETRY, FALLTHROUGH_RETRY,
    LOGGING_DEFAULT_RETRY, LOGGING_DOWNGRADING_RETRY, LOGGING_FALLTHROUGH_RETRY)
    .mkString(QUERY_OPTIONS_PREFIX + """retryPolicy\s*=\s*(""","|",")").r

  val PREPARE_STATEMENT_PREFIX = QUERY_OPTIONS_PREFIX + "prepare"
  val PREPARE_STATEMENT_PATTERN = (QUERY_OPTIONS_PREFIX + """prepare\[([^]]+)\]\s*=\s*(.+)""").r

  val BIND_PREFIX = QUERY_OPTIONS_PREFIX + "bind"
  val BIND_PATTERN = (QUERY_OPTIONS_PREFIX + """bind\[([^]]+)\]=(.+)""").r

  val defaultRetryPolicy = Policies.defaultRetryPolicy()
  val downgradingConsistencyRetryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE
  val fallThroughRetryPolicy = FallthroughRetryPolicy.INSTANCE
  val loggingDefaultRetryPolicy = new LoggingRetryPolicy(defaultRetryPolicy)
  val loggingDownGradingRetryPolicy = new LoggingRetryPolicy(downgradingConsistencyRetryPolicy)
  val loggingFallThrougRetryPolicy = new LoggingRetryPolicy(fallThroughRetryPolicy)

  val preparedStatements : mutable.Map[String,PreparedStatement] = mutable.Map[String,PreparedStatement]()

  val logger = LoggerFactory.getLogger(classOf[CassandraInterpreterHelper])

  val paragraphParser = new ParagraphParser
  val boundValuesParser = new BoundValuesParser

  def toScalaList[A](list: java.util.List[A]): List[A] = list.asScala.toList
  def toJavaList[A](list: List[A]): java.util.List[A] = list.asJava
}

class CassandraInterpreterHelper(val session: Session)  {
 
  import CassandraInterpreterHelper._

  def interpret(session:Session, stringStatements : String, context: InterpreterContext): InterpreterResult = {

    logger.info(s"Executing CQL statements : \n\n$stringStatements\n")

    try {
      val protocolVersion = session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersionEnum

      val queries:List[AnyBlock] = parseInput(stringStatements)

      val queryOptions = extractQueryOptions(queries
        .filter(_.blockType == ParameterBlock)
        .map(_.get[QueryParameters]))

      logger.info(s"Current Cassandra query options = $queryOptions")

      val queryStatements = queries .filter(_.blockType == StatementBlock).map(_.get[QueryStatement])

      //Remove prepared statements
      queryStatements
        .filter(_.statementType == RPS)
        .map(_.getStatement[RemovePrepareStm])
        .foreach(remove => {
          logger.debug(s"Removing prepared statement '${remove.name}'")
          preparedStatements.remove(remove.name)
        })

      //Update prepared statement maps
      queryStatements
        .filter(_.statementType == PS)
        .map(_.getStatement[PrepareStm])
        .foreach(statement => {
          logger.debug(s"Get or prepare statement '${statement.name}' : ${statement.query}")
          preparedStatements.getOrElseUpdate(statement.name,session.prepare(statement.query))
        })

      val statements: List[Statement] = queryStatements
        .filter(st => (st.statementType != PS) && (st.statementType != RPS))
        .map{
          case x:SimpleStm => generateSimpleStatement(x, queryOptions, context)
          case x:BatchStm => {
            val builtStatements: List[Statement] = x.statements.map {
              case st:SimpleStm => generateSimpleStatement(st, queryOptions, context)
              case st:BoundStm => generateBoundStatement(st, queryOptions, context)
              case _ => throw new InterpreterException(s"Unknown statement type")
            }
            generateBatchStatement(x.batchType, queryOptions, builtStatements)
          }
          case x:BoundStm => generateBoundStatement(x, queryOptions, context)
          case _ => throw new InterpreterException(s"Unknown statement type")
       }

      val resultSets: List[ResultSet] = for (statement <- statements) yield session.execute(statement)

      if (resultSets.size > 0) {
        buildResult(resultSets.last, protocolVersion)
      } else {
        new InterpreterResult(Code.SUCCESS, "No Result")
      }

    } catch {
      case dex: DriverException => {
        logger.error(dex.getMessage, dex)
        new InterpreterResult(Code.ERROR, parseException(dex))
      }
      case pex:ParsingException => {
        logger.error(pex.getMessage, pex)
        new InterpreterResult(Code.ERROR, pex.getMessage)
      }
      case iex: InterpreterException => {
        logger.error(iex.getMessage, iex)
        new InterpreterResult(Code.ERROR, iex.getMessage)
      }
      case ex: java.lang.Exception => {
        logger.error(ex.getMessage, ex)
        new InterpreterResult(Code.ERROR, parseException(ex))
      }
    }
  }

  def buildResult(lastResultSet: ResultSet, protocolVersion: ProtocolVersion): InterpreterResult = {
    val output = new StringBuilder()
    val rows: List[Row] = lastResultSet.all().toList
    val columnsDefinitions:List[(String,DataType)] = lastResultSet
      .getColumnDefinitions
      .asList
      .toList // Java list -> Scala list
      .map(definition => (definition.getName,definition.getType))

    // Create table headers
    output
      .append("%table ")
      .append(columnsDefinitions.map{case(columnName,_) => columnName}.mkString("\t")).append("\n")

    // Deserialize Data
    rows.foreach {
      row => {
        val data = columnsDefinitions.map{
          case (name, dataType) => {
            if(row.isNull(name)) null else dataType.deserialize(row.getBytesUnsafe(name), protocolVersion)
          }
        }
        output.append(data.mkString("\t")).append("\n")
      }
    }

    val result: String = output.toString()
    logger.debug(s"CQL result : \n\n$result\n")
    new InterpreterResult(Code.SUCCESS, result)
  }

  def parseInput(input:String): List[AnyBlock] = {
    val parsingResult: ParagraphParser#ParseResult[List[AnyBlock]] = paragraphParser.parseAll(paragraphParser.queries, input)
    parsingResult match {
      case paragraphParser.Success(blocks,_) => blocks
      case paragraphParser.Failure(msg,next) => {
        throw new InterpreterException(s"Error parsing input:\n\t'$input'\nDid you forget to add ; (semi-colon) at the end of each CQL statement ?")
      }
      case paragraphParser.Error(msg,next) => {
        throw new InterpreterException(s"Error parsing input:\n\t'$input'\nDid you forget to add ; (semi-colon) at the end of each CQL statement ?")
      }
      case _ => throw new InterpreterException(s"Error parsing input: $input")
    }
  }

  def extractQueryOptions(parameters: List[QueryParameters]): CassandraQueryOptions = {

    logger.debug(s"Extracting query options from $parameters")

    val consistency: Option[ConsistencyLevel] = parameters
      .filter(_.paramType == CS)
      .map(_.getParam[Consistency])
      .flatMap(x => Option(x.value))
      .headOption


    val serialConsistency: Option[ConsistencyLevel] = parameters
      .filter(_.paramType == SCS)
      .map(_.getParam[SerialConsistency])
      .flatMap(x => Option(x.value))
      .headOption

    val timestamp: Option[Long] = parameters
      .filter(_.paramType == TS)
      .map(_.getParam[Timestamp])
      .flatMap(x => Option(x.value))
      .headOption

    val retryPolicy: Option[RetryPolicy] = parameters
      .filter(_.paramType == RP)
      .map(_.getParam[RetryPolicy])
      .headOption

    CassandraQueryOptions(consistency,serialConsistency, timestamp, retryPolicy)
  }

  def generateSimpleStatement(st: SimpleStm, options: CassandraQueryOptions,context: InterpreterContext): SimpleStatement = {
    logger.debug(s"Generating simple statement : '${st.text}'")
    val statement = new SimpleStatement(maybeExtractVariables(st.text, context))
    applyQueryOptions(options, statement)
    statement
  }

  def generateBoundStatement(st: BoundStm, options: CassandraQueryOptions,context: InterpreterContext): BoundStatement = {
    logger.debug(s"Generating bound statement with name : '${st.name}' and bound values : ${st.values}")
    preparedStatements.get(st.name) match {
      case Some(ps) => {
        val boundValues = maybeExtractVariables(st.values, context)
        createBoundStatement(st.name, ps, boundValues)
      }
      case None => throw new InterpreterException(s"The statement '${st.name}' can not be bound to values. " +
          s"Are you sure you did prepare it with @prepare[${st.name}] ?")
    }
  }

  def generateBatchStatement(batchType: BatchStatement.Type, options: CassandraQueryOptions, statements: List[Statement]): BatchStatement = {
    logger.debug(s"""Generating batch statement of type '${batchType} for ${statements.mkString(",")}'""")
    val batch = new BatchStatement(batchType)
    statements.foreach(batch.add(_))
    applyQueryOptions(options, batch)
    batch
  }

  def maybeExtractVariables(statement: String, context: InterpreterContext): String = {

    def extractVariableAndDefaultValue(statement: String, exp: String):String = {
      exp match {
        case MULTIPLE_CHOICES_VARIABLE_DEFINITION_PATTERN(variable,choices) => {
          val escapedExp: String = exp.replaceAll( """\{""", """\\{""").replaceAll( """\}""", """\\}""").replaceAll("""\|""","""\\|""")
          val listChoices:List[String] = choices.trim.split(CHOICES_SEPARATOR).toList
          val paramOptions= listChoices.map(choice => new ParamOption(choice, choice))
          val selected = context.getGui.select(variable, listChoices.head, paramOptions.toArray)
          statement.replaceAll(escapedExp,selected.toString)
        }
        case SIMPLE_VARIABLE_DEFINITION_PATTERN(variable,defaultVal) => {
          val escapedExp: String = exp.replaceAll( """\{""", """\\{""").replaceAll( """\}""", """\\}""")
          val value = context.getGui.input(variable,defaultVal)
          statement.replaceAll(escapedExp,value.toString)
        }
        case _ => throw new ParsingException(s"Invalid bound variable definition for '$exp' in '$statement'. It should be of form 'variable=defaultValue' or 'variable=value1|value2|...|valueN'")
      }
    }

    VARIABLE_PATTERN.findAllIn(statement).foldLeft(statement)(extractVariableAndDefaultValue _)
  }

  def applyQueryOptions(options: CassandraQueryOptions, statement: Statement): Unit = {
    options.consistency.foreach(statement.setConsistencyLevel(_))
    options.serialConsistency.foreach(statement.setSerialConsistencyLevel(_))
    options.timestamp.foreach(statement.setDefaultTimestamp(_))
    options.retryPolicy.foreach {
      case DefaultRetryPolicy => statement.setRetryPolicy(defaultRetryPolicy)
      case DowngradingRetryPolicy => statement.setRetryPolicy(downgradingConsistencyRetryPolicy)
      case FallThroughRetryPolicy => statement.setRetryPolicy(fallThroughRetryPolicy)
      case LoggingDefaultRetryPolicy => statement.setRetryPolicy(loggingDefaultRetryPolicy)
      case LoggingDowngradingRetryPolicy => statement.setRetryPolicy(loggingDownGradingRetryPolicy)
      case LoggingFallThroughRetryPolicy => statement.setRetryPolicy(loggingFallThrougRetryPolicy)
      case _ => throw new InterpreterException(s"""Unknown retry policy ${options.retryPolicy.getOrElse("???")}""")
    }
  }

  private def createBoundStatement(name: String, ps: PreparedStatement, rawBoundValues: String): BoundStatement = {
    val dataTypes = ps.getVariables.toList
      .map(cfDef => cfDef.getType)

    val boundValuesAsText = parseBoundValues(name,rawBoundValues)

    if(dataTypes.size != boundValuesAsText.size) throw new InterpreterException(s"Invalid @bind values for prepared statement '$name'. " +
      s"Prepared parameters has ${dataTypes.size} variables whereas bound values have ${boundValuesAsText.size} parameters ...")

    val convertedValues: List[AnyRef] = boundValuesAsText
      .zip(dataTypes).map {
        case (value, dataType) => {
          if(value.trim == "null") {
            null
          } else {
            dataType.getName match {
            case (ASCII | TEXT | VARCHAR) => value.trim.replaceAll("(?<!')'","")
            case (INT | VARINT) => value.trim.toInt
            case (BIGINT | COUNTER) => value.trim.toLong
            case BLOB => ByteBuffer.wrap(value.trim.getBytes)
            case BOOLEAN => value.trim.toBoolean
            case DECIMAL => BigDecimal(value.trim)
            case DOUBLE => value.trim.toDouble
            case FLOAT => value.trim.toFloat
            case INET => InetAddress.getByName(value.trim)
            case TIMESTAMP => parseDate(value.trim)
            case (UUID | TIMEUUID) => java.util.UUID.fromString(value.trim)
            case LIST => dataType.parse(boundValuesParser.parse(boundValuesParser.list, value).get)
            case SET => dataType.parse(boundValuesParser.parse(boundValuesParser.set, value).get)
            case MAP => dataType.parse(boundValuesParser.parse(boundValuesParser.map, value).get)
            case UDT => dataType.parse(boundValuesParser.parse(boundValuesParser.udt, value).get)
            case TUPLE => dataType.parse(boundValuesParser.parse(boundValuesParser.tuple, value).get)
            case _ => throw new InterpreterException(s"Cannot parse data of type : ${dataType.toString}")
          }
        }
      }
    }.asInstanceOf[List[AnyRef]]

    ps.bind(convertedValues.toArray: _*)
  }

  private def parseBoundValues(psName: String, boundValues: String): List[String] = {
    val result: BoundValuesParser#ParseResult[List[String]] = boundValuesParser.parseAll(boundValuesParser.values, boundValues)

    result match {
      case boundValuesParser.Success(list,_) => list
      case _ => throw new InterpreterException(s"Cannot parse bound values for prepared statement '$psName' : $boundValues. Did you forget to wrap text with ' (simple quote) ?")
    }
  }

  def parseDate(dateString: String): Date = {
    dateString match {
      case boundValuesParser.STANDARD_DATE_PATTERN(datePattern) => boundValuesParser.standardDateFormat.parse(datePattern)
      case boundValuesParser.ACCURATE_DATE_PATTERN(datePattern) => boundValuesParser.accurateDateFormat.parse(datePattern)
      case _ => throw new InterpreterException(s"Cannot parse date '$dateString'. " +
        s"Accepted formats : ${boundValuesParser.standardDateFormat.toPattern} OR ${boundValuesParser.accurateDateFormat.toPattern}");
    }
  }

  def parseException(ex: Exception): String = {
    val msg = new StringBuilder()
      .append(ex.getClass.getCanonicalName)
      .append(Option(ex.getMessage) match {
        case Some(x) => x + " : "
        case None => ""})
      .append("\n")
    ex.getStackTrace.foreach{
      stack => {
        val stackLine: String = s"""\t at ${stack.getClassName}.${stack.getMethodName}(${stack.getFileName}):${stack.getLineNumber}"""
        msg.append(stackLine).append("\n")
      }
    }
    msg.toString()
  }

}
