package ru.otus.spark.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import scala.collection.JavaConverters._

class DefaultSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    PostgresTable.schema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = new PostgresTable(
    properties.get("tableName")
  ) // TODO: Error handling

}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new PostgresPartitionCalcBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("public", LongType)
}

object PartitionSize {
  val defaultValue: Int = 5
}

case class ConnectionProperties(
    url: String,
    user: String,
    password: String,
    tableName: String,
    partitionSize: Int = PartitionSize.defaultValue,
)

/** Read */
class PostgresPartitionCalcBuilder(options: CaseInsensitiveStringMap)
    extends ScanBuilder {
  override def build(): Scan = new PostgresPartitionCalc(
    ConnectionProperties(
      options.get("url"),
      options.get("user"),
      options.get("password"),
      options.get("tableName"),
      options.get("partitionSize").toInt,
    )
  )
}

class TableRowsCounter(connectionProperties: ConnectionProperties) {
  private val connection = DriverManager.getConnection(
    connectionProperties.url, connectionProperties.user, connectionProperties.password
  )
  private val statement = connection.createStatement()
  private val query = s"select count(*) from ${connectionProperties.tableName}"
  private val resultSet = statement.executeQuery(query)

  def next(): Boolean = resultSet.next()

  def getTotalRows: Long = resultSet.getLong(1)
}

//TODO: возможно можно пойти через партиции
class TableGetPartitionCounter(connectionProperties: ConnectionProperties) {
  private val connection = DriverManager.getConnection(
    connectionProperties.url, connectionProperties.user, connectionProperties.password
  )
  private val query =
    s"""SELECT
            nmsp_child.nspname  AS child_schema,
            child.relname       AS child
        FROM pg_inherits
            JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
        WHERE nmsp_parent.nspname = '${PostgresTable.schema}'
        AND parent.relname='${connectionProperties.tableName}'"""

  var res: List[String] = List()
  private val rs = connection.createStatement().executeQuery(query)
  while (rs.next) {
    res = res :+ rs.getString("partition")
  }
}

case class PostgresPartition(val pFrom: Long, val pTo: Long) extends InputPartition

class PostgresPartitionCalc(connectionProperties: ConnectionProperties)
    extends Scan
    with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val postgresUtils = new TableRowsCounter(connectionProperties)
    val totalRows = if (postgresUtils.next()) postgresUtils.getTotalRows else 0

    val partitionsNumber = Math.ceil(totalRows * 1.0 / connectionProperties.partitionSize).toInt
    var partitions = new Array[InputPartition](partitionsNumber)
    for (i <- 0 until partitionsNumber) {
      val pFrom = i * connectionProperties.partitionSize
      partitions(i) = PostgresPartition(pFrom, pFrom + connectionProperties.partitionSize)
    }
    partitions

  }

  override def createReaderFactory(): PartitionReaderFactory =
    new PostgresPartitionReaderFactory(connectionProperties)
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new PostgresPartitionReader(connectionProperties,
      partition.asInstanceOf[PostgresPartition].pFrom, partition.asInstanceOf[PostgresPartition].pTo)
}

//забираем данные из конкретной партиции
class PostgresPartitionReader(connectionProperties: ConnectionProperties,
                              pFrom: Long,
                              pTo: Long)
    extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  private val statement = connection.createStatement()
  private val query = s"select * from ${connectionProperties.tableName} order by 1 offset $pFrom limit ${pTo - pFrom}"
  private val resultSet: ResultSet = statement.executeQuery(query)

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()
}

/** Write */
class PostgresWriteBuilder(options: CaseInsensitiveStringMap)
    extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(
    ConnectionProperties(
      options.get("url"),
      options.get("user"),
      options.get("password"),
      options.get("tableName"),
    )
  )
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties)
    extends BatchWrite {
  override def createBatchWriterFactory(
      physicalWriteInfo: PhysicalWriteInfo
  ): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(
      writerCommitMessages: Array[WriterCommitMessage]
  ): Unit = {}

  override def abort(
      writerCommitMessages: Array[WriterCommitMessage]
  ): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties)
    extends DataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties)
    extends DataWriter[InternalRow] {

  val connection: Connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement: PreparedStatement =
    connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}
