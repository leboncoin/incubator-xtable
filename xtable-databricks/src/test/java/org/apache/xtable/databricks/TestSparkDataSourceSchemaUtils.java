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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

class TestSparkDataSourceSchemaUtils {

  private static InternalField field(String name, InternalType type, String comment) {
    return InternalField.builder()
        .name(name)
        .schema(
            InternalSchema.builder()
                .name(name)
                .dataType(type)
                .comment(comment)
                .isNullable(true)
                .build())
        .build();
  }

  @Test
  void convertsSchemaWithCommentsToStructJson() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("root")
            .dataType(InternalType.RECORD)
            .isNullable(true)
            .fields(
                Arrays.asList(
                    field("id", InternalType.INT, "primary key"),
                    field("name", InternalType.STRING, null)))
            .build();

    String json = SparkDataSourceSchemaUtils.convertToSparkSchemaJson(schema, null);

    assertEquals(
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,"
            + "\"metadata\":{\"comment\":\"primary key\"}},"
            + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}"
            + "]}",
        json);
  }

  @Test
  void reordersPartitionColumnsLast() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("root")
            .dataType(InternalType.RECORD)
            .isNullable(true)
            .fields(
                Arrays.asList(
                    field("dt", InternalType.STRING, null), field("id", InternalType.INT, null)))
            .build();

    String json =
        SparkDataSourceSchemaUtils.convertToSparkSchemaJson(
            schema, Collections.singletonList("dt"));

    // "id" (data) before "dt" (partition)
    assertTrue(json.indexOf("\"id\"") < json.indexOf("\"dt\""), json);
  }

  @Test
  void buildsSchemaPropertiesWithChunkingAndPartCols() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("root")
            .dataType(InternalType.RECORD)
            .isNullable(true)
            .fields(
                Arrays.asList(
                    field("id", InternalType.INT, null), field("dt", InternalType.STRING, null)))
            .build();

    Map<String, String> properties =
        SparkDataSourceSchemaUtils.getSparkSchemaProperties(
            schema, Collections.singletonList("dt"), 10);

    String expectedJson =
        SparkDataSourceSchemaUtils.convertToSparkSchemaJson(
            schema, Collections.singletonList("dt"));
    int expectedParts = (expectedJson.length() + 10 - 1) / 10;

    assertEquals(
        String.valueOf(expectedParts), properties.get("spark.sql.sources.schema.numParts"));
    assertEquals("1", properties.get("spark.sql.sources.schema.numPartCols"));
    assertEquals("dt", properties.get("spark.sql.sources.schema.partCol.0"));

    StringBuilder reassembled = new StringBuilder();
    for (int i = 0; i < expectedParts; i++) {
      reassembled.append(properties.get("spark.sql.sources.schema.part." + i));
    }
    assertEquals(expectedJson, reassembled.toString());
  }
}
