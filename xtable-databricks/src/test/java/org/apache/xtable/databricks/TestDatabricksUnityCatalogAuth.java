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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.databricks.sdk.service.sql.StatementExecutionAPI;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;

/** Authentication wiring: static token, OIDC federation (file-oidc) and OAuth M2M. */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestDatabricksUnityCatalogAuth {

  private static final String HOST = "https://example.cloud.databricks.com";
  private static final String AUDIENCE = "https://accounts.cloud.databricks.com";

  @Mock private StatementExecutionAPI mockStatementExecution;
  @Mock private TablesAPI mockTablesApi;
  @Mock private SchemasAPI mockSchemasApi;

  @TempDir Path tempDir;

  @Test
  void oidcTokenFilePathSelectsFileOidc() throws IOException {
    Path tokenFile = writeToken("a.jwt.value");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());

    DatabricksConfig dbConfig = buildConfig(props);

    assertEquals("file-oidc", dbConfig.getAuthType());
    assertEquals(tokenFile.toString(), dbConfig.getOidcTokenFilepath());
    assertEquals("app-id", dbConfig.getClientId());
    assertNull(dbConfig.getClientSecret());
  }

  @Test
  void tokenAudienceIsForwardedWhenProvided() throws IOException {
    Path tokenFile = writeToken("a.jwt.value");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());
    props.put(DatabricksUnityCatalogConfig.TOKEN_AUDIENCE, AUDIENCE);

    DatabricksConfig dbConfig = buildConfig(props);

    assertEquals(AUDIENCE, dbConfig.getTokenAudience());
  }

  @Test
  void staticTokenTakesPrecedenceOverOidc() throws IOException {
    Path tokenFile = writeToken("a.jwt.value");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.TOKEN, "dapi-pat");
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());

    DatabricksConfig dbConfig = buildConfig(props);

    assertEquals("dapi-pat", dbConfig.getToken());
    assertNull(dbConfig.getOidcTokenFilepath());
  }

  @Test
  void oidcTakesPrecedenceOverClientSecret() throws IOException {
    Path tokenFile = writeToken("a.jwt.value");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.CLIENT_SECRET, "a-secret");
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());

    DatabricksConfig dbConfig = buildConfig(props);

    assertEquals("file-oidc", dbConfig.getAuthType());
    assertNull(dbConfig.getClientSecret());
  }

  @Test
  void clientCredentialsStillSelectOauthM2m() {
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.CLIENT_SECRET, "a-secret");

    DatabricksConfig dbConfig = buildConfig(props);

    assertEquals("oauth-m2m", dbConfig.getAuthType());
    assertEquals("a-secret", dbConfig.getClientSecret());
  }

  @Test
  void explicitAuthTypeIsNotOverridden() throws IOException {
    Path tokenFile = writeToken("a.jwt.value");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.AUTH_TYPE, "oauth-m2m");
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());

    DatabricksConfig dbConfig = buildConfig(props);

    assertEquals("oauth-m2m", dbConfig.getAuthType());
    assertEquals(tokenFile.toString(), dbConfig.getOidcTokenFilepath());
  }

  @Test
  void initFailsWhenClientIdIsMissing() throws IOException {
    Path tokenFile = writeToken("a.jwt.value");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());

    CatalogSyncException exception = assertThrows(CatalogSyncException.class, () -> init(props));
    assertTrue(exception.getMessage().contains(DatabricksUnityCatalogConfig.CLIENT_ID));
  }

  @Test
  void initFailsWhenTokenFileIsMissing() {
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(
        DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tempDir.resolve("absent").toString());

    CatalogSyncException exception = assertThrows(CatalogSyncException.class, () -> init(props));
    assertTrue(exception.getMessage().contains("does not exist"));
  }

  @Test
  void initFailsWhenTokenFileIsEmpty() throws IOException {
    Path tokenFile = writeToken("   \n");
    Map<String, String> props = baseProps();
    props.put(DatabricksUnityCatalogConfig.CLIENT_ID, "app-id");
    props.put(DatabricksUnityCatalogConfig.OIDC_TOKEN_FILE_PATH, tokenFile.toString());

    CatalogSyncException exception = assertThrows(CatalogSyncException.class, () -> init(props));
    assertTrue(exception.getMessage().contains("is empty"));
  }

  private Path writeToken(String content) throws IOException {
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, content.getBytes(StandardCharsets.UTF_8));
    return tokenFile;
  }

  private Map<String, String> baseProps() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, HOST);
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    return props;
  }

  private static ExternalCatalogConfig catalogConfig(Map<String, String> props) {
    return ExternalCatalogConfig.builder()
        .catalogId("uc")
        .catalogType(CatalogType.DATABRICKS_UC)
        .catalogProperties(props)
        .build();
  }

  private DatabricksUnityCatalogSyncClient init(Map<String, String> props) {
    return new DatabricksUnityCatalogSyncClient(
        catalogConfig(props),
        TableFormat.DELTA,
        new Configuration(),
        mockStatementExecution,
        mockTablesApi,
        mockSchemasApi);
  }

  private DatabricksConfig buildConfig(Map<String, String> props) {
    DatabricksUnityCatalogSyncClient client = init(props);
    return client.buildConfig(DatabricksUnityCatalogConfig.from(catalogConfig(props)));
  }
}
