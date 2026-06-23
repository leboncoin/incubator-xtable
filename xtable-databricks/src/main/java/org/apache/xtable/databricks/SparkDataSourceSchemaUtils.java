/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.databricks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;

/**
 * Builds the Spark DataSource `spark.sql.sources.schema.*` table properties from an {@link
 * InternalSchema}, replicating Hudi's `SparkDataSourceTableUtils`/`AvroToSparkJson` behaviour so
 * that Unity Catalog tables carry the same StructType JSON (including per-column comments).
 */
final class SparkDataSourceSchemaUtils {

  private SparkDataSourceSchemaUtils() {}

  /**
   * Produce the `spark.sql.sources.schema.*` properties for the given schema. Data columns are
   * emitted first and partition columns last, matching the Spark DataSource table convention used
   * by Hudi's Hive sync.
   */
  static Map<String, String> getSparkSchemaProperties(
      InternalSchema schema, List<String> partitionFieldNames, int schemaStringLengthThreshold) {
    Map<String, String> properties = new HashMap<>();
    String schemaJson = convertToSparkSchemaJson(schema, partitionFieldNames);

    int numParts =
        (schemaJson.length() + schemaStringLengthThreshold - 1) / schemaStringLengthThreshold;
    properties.put("spark.sql.sources.schema.numParts", String.valueOf(numParts));
    for (int i = 0; i < numParts; i++) {
      int start = i * schemaStringLengthThreshold;
      int end = Math.min(start + schemaStringLengthThreshold, schemaJson.length());
      properties.put("spark.sql.sources.schema.part." + i, schemaJson.substring(start, end));
    }
    if (partitionFieldNames != null && !partitionFieldNames.isEmpty()) {
      properties.put(
          "spark.sql.sources.schema.numPartCols", String.valueOf(partitionFieldNames.size()));
      for (int i = 0; i < partitionFieldNames.size(); i++) {
        properties.put("spark.sql.sources.schema.partCol." + i, partitionFieldNames.get(i));
      }
    }
    return properties;
  }

  /** Convert an {@link InternalSchema} record to the Spark StructType JSON representation. */
  static String convertToSparkSchemaJson(InternalSchema schema, List<String> partitionFieldNames) {
    if (schema == null || schema.getFields() == null) {
      throw new CatalogSyncException("Top-level schema must be a record");
    }
    List<InternalField> ordered = reorderFields(schema.getFields(), partitionFieldNames);
    return structJson(ordered);
  }

  // Data columns first, partition columns last (Spark DataSource table convention).
  private static List<InternalField> reorderFields(
      List<InternalField> fields, List<String> partitionFieldNames) {
    if (partitionFieldNames == null || partitionFieldNames.isEmpty()) {
      return fields;
    }
    List<InternalField> dataFields = new ArrayList<>();
    List<InternalField> partitionFields = new ArrayList<>();
    for (InternalField field : fields) {
      if (partitionFieldNames.contains(field.getName())) {
        partitionFields.add(field);
      } else {
        dataFields.add(field);
      }
    }
    List<InternalField> reordered = new ArrayList<>(dataFields);
    reordered.addAll(partitionFields);
    return reordered;
  }

  private static String structJson(List<InternalField> fields) {
    StringBuilder builder = new StringBuilder("{\"type\":\"struct\",\"fields\":[");
    boolean first = true;
    for (InternalField field : fields) {
      if (!first) {
        builder.append(",");
      }
      builder.append(fieldJson(field));
      first = false;
    }
    builder.append("]}");
    return builder.toString();
  }

  private static String fieldJson(InternalField field) {
    InternalSchema fieldSchema = field.getSchema();
    StringBuilder metadata = new StringBuilder("{");
    String comment = fieldSchema.getComment();
    if (comment != null && !comment.trim().isEmpty()) {
      metadata.append("\"comment\":\"").append(escapeJson(comment)).append("\"");
    }
    metadata.append("}");
    return "{\"name\":\""
        + escapeJson(field.getName())
        + "\",\"type\":"
        + typeJson(fieldSchema)
        + ",\"nullable\":"
        + fieldSchema.isNullable()
        + ",\"metadata\":"
        + metadata
        + "}";
  }

  private static String typeJson(InternalSchema schema) {
    switch (schema.getDataType()) {
      case ENUM:
      case STRING:
        return "\"string\"";
      case INT:
        return "\"integer\"";
      case LONG:
        return "\"long\"";
      case BYTES:
      case FIXED:
      case UUID:
        return "\"binary\"";
      case BOOLEAN:
        return "\"boolean\"";
      case FLOAT:
        return "\"float\"";
      case DOUBLE:
        return "\"double\"";
      case DATE:
        return "\"date\"";
      case TIMESTAMP:
        return "\"timestamp\"";
      case TIMESTAMP_NTZ:
        return "\"timestamp_ntz\"";
      case DECIMAL:
        int precision =
            (int) schema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) schema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
        return "\"decimal(" + precision + "," + scale + ")\"";
      case RECORD:
        return structJson(schema.getFields());
      case LIST:
        InternalField element =
            childField(schema, InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME);
        return "{\"type\":\"array\",\"elementType\":"
            + typeJson(element.getSchema())
            + ",\"containsNull\":"
            + element.getSchema().isNullable()
            + "}";
      case MAP:
        InternalField key = childField(schema, InternalField.Constants.MAP_KEY_FIELD_NAME);
        InternalField value = childField(schema, InternalField.Constants.MAP_VALUE_FIELD_NAME);
        return "{\"type\":\"map\",\"keyType\":"
            + typeJson(key.getSchema())
            + ",\"valueType\":"
            + typeJson(value.getSchema())
            + ",\"valueContainsNull\":"
            + value.getSchema().isNullable()
            + "}";
      default:
        throw new CatalogSyncException("Unsupported type: " + schema.getDataType());
    }
  }

  private static InternalField childField(InternalSchema schema, String fieldName) {
    return schema.getFields().stream()
        .filter(field -> fieldName.equals(field.getName()))
        .findFirst()
        .orElseThrow(() -> new CatalogSyncException("Invalid " + schema.getDataType() + " schema"));
  }

  private static String escapeJson(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          escaped.append("\\\"");
          break;
        case '\\':
          escaped.append("\\\\");
          break;
        case '\b':
          escaped.append("\\b");
          break;
        case '\f':
          escaped.append("\\f");
          break;
        case '\n':
          escaped.append("\\n");
          break;
        case '\r':
          escaped.append("\\r");
          break;
        case '\t':
          escaped.append("\\t");
          break;
        default:
          if (c < 0x20) {
            escaped.append(String.format("\\u%04x", (int) c));
          } else {
            escaped.append(c);
          }
      }
    }
    return escaped.toString();
  }
}
