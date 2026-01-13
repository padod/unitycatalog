package io.unitycatalog.spark.utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

import java.io.BufferedReader
import java.io.InputStreamReader
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}

/**
 * Metadata extracted from Delta log including schema, partition info, and table properties.
 */
case class DeltaTableMetadata(
    schema: StructType,
    partitionColumns: Seq[String],
    description: Option[String] = None,
    configuration: Map[String, String] = Map.empty,
    createdTime: Option[Long] = None
)

/**
 * Utility to read schema from Delta Lake transaction log.
 * This reads the _delta_log directory directly without requiring Delta Lake library.
 */
object DeltaSchemaReader extends Logging {

  private val objectMapper = new ObjectMapper()

  /**
   * Read the current schema from a Delta table's transaction log.
   *
   * @param location The storage location of the Delta table
   * @param hadoopConf Hadoop configuration with credentials
   * @return Option[StructType] - the schema if found, None otherwise
   */
  def readSchema(location: String, hadoopConf: Configuration): Option[StructType] = {
    readMetadata(location, hadoopConf).map(_.schema)
  }

  /**
   * Read full metadata from a Delta table's transaction log including schema and partition columns.
   *
   * @param location The storage location of the Delta table
   * @param hadoopConf Hadoop configuration with credentials
   * @return Option[DeltaTableMetadata] - the metadata if found, None otherwise
   */
  def readMetadata(location: String, hadoopConf: Configuration): Option[DeltaTableMetadata] = {
    Try {
      val deltaLogPath = new Path(location, "_delta_log")
      val fs = FileSystem.get(new Path(location).toUri, hadoopConf)

      if (!fs.exists(deltaLogPath)) {
        logDebug(s"Delta log not found at $deltaLogPath")
        return None
      }

      // Find the latest checkpoint or read all JSON files
      val checkpointMetadata = readMetadataFromCheckpoint(fs, deltaLogPath)
      if (checkpointMetadata.isDefined) {
        return checkpointMetadata
      }

      // Read from JSON commit files
      readMetadataFromCommitFiles(fs, deltaLogPath)
    } match {
      case Success(metadata) => metadata
      case Failure(e) =>
        logWarning(s"Failed to read Delta metadata from $location: ${e.getMessage}")
        None
    }
  }

  /**
   * Read metadata from checkpoint file if available.
   */
  private def readMetadataFromCheckpoint(fs: FileSystem, deltaLogPath: Path): Option[DeltaTableMetadata] = {
    val lastCheckpointPath = new Path(deltaLogPath, "_last_checkpoint")
    if (!fs.exists(lastCheckpointPath)) {
      return None
    }

    Try {
      val reader = new BufferedReader(new InputStreamReader(fs.open(lastCheckpointPath)))
      try {
        val content = reader.readLine()
        val checkpointInfo = objectMapper.readTree(content)
        val version = checkpointInfo.get("version").asLong()

        // Read the checkpoint parquet file - for now, fall back to JSON files
        // Checkpoint files are in Parquet format which is more complex to parse
        // TODO: Implement parquet checkpoint reading if needed
        None
      } finally {
        reader.close()
      }
    }.getOrElse(None)
  }

  /**
   * Read full metadata from JSON commit files.
   * Schema and partition columns are defined in 'metaData' actions.
   */
  private def readMetadataFromCommitFiles(fs: FileSystem, deltaLogPath: Path): Option[DeltaTableMetadata] = {
    // List all JSON files and sort by version (descending to get latest first)
    val jsonFiles = fs.listStatus(deltaLogPath)
      .filter(f => f.getPath.getName.endsWith(".json"))
      .sortBy(f => -extractVersion(f.getPath.getName))

    // Read files from newest to oldest, looking for metaData action
    for (fileStatus <- jsonFiles) {
      val metadata = readMetadataFromJsonFile(fs, fileStatus.getPath)
      if (metadata.isDefined) {
        return metadata
      }
    }
    None
  }

  /**
   * Extract version number from filename like "00000000000000000001.json"
   */
  private def extractVersion(filename: String): Long = {
    Try(filename.stripSuffix(".json").toLong).getOrElse(-1L)
  }

  /**
   * Read full metadata from a single Delta log JSON file.
   * Each line is a separate JSON action.
   * Extracts schema, partition columns, description, configuration, and createdTime.
   */
  private def readMetadataFromJsonFile(fs: FileSystem, path: Path): Option[DeltaTableMetadata] = {
    Try {
      val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
      try {
        var line = reader.readLine()
        while (line != null) {
          val action = objectMapper.readTree(line)
          if (action.has("metaData")) {
            val metaData = action.get("metaData")
            if (metaData.has("schemaString")) {
              val schemaString = metaData.get("schemaString").asText()
              val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]

              // Extract partition columns
              val partitionColumns = if (metaData.has("partitionColumns")) {
                val partColsNode = metaData.get("partitionColumns")
                val cols = ArrayBuffer.empty[String]
                val iter = partColsNode.elements()
                while (iter.hasNext) {
                  cols += iter.next().asText()
                }
                cols.toSeq
              } else {
                Seq.empty[String]
              }

              // Extract table description
              val description = if (metaData.has("description") && !metaData.get("description").isNull) {
                Some(metaData.get("description").asText())
              } else {
                None
              }

              // Extract table configuration/properties
              val configuration = if (metaData.has("configuration")) {
                val configNode = metaData.get("configuration")
                val configMap = scala.collection.mutable.Map.empty[String, String]
                val fields = configNode.fields()
                while (fields.hasNext) {
                  val entry = fields.next()
                  configMap(entry.getKey) = entry.getValue.asText()
                }
                configMap.toMap
              } else {
                Map.empty[String, String]
              }

              // Extract createdTime
              val createdTime = if (metaData.has("createdTime") && !metaData.get("createdTime").isNull) {
                Some(metaData.get("createdTime").asLong())
              } else {
                None
              }

              return Some(DeltaTableMetadata(
                schema = schema,
                partitionColumns = partitionColumns,
                description = description,
                configuration = configuration,
                createdTime = createdTime
              ))
            }
          }
          line = reader.readLine()
        }
        None
      } finally {
        reader.close()
      }
    }.getOrElse(None)
  }

  /**
   * Compare two schemas for equality (ignoring metadata).
   */
  def schemasEqual(schema1: StructType, schema2: StructType): Boolean = {
    if (schema1.length != schema2.length) return false

    schema1.fields.zip(schema2.fields).forall { case (f1, f2) =>
      f1.name == f2.name &&
        f1.dataType == f2.dataType &&
        f1.nullable == f2.nullable
    }
  }

  /**
   * Convert StructType fields to a comparable format (name, type, nullable).
   */
  def schemaToComparable(schema: StructType): Seq[(String, String, Boolean)] = {
    schema.fields.map(f => (f.name, f.dataType.simpleString, f.nullable))
  }
}