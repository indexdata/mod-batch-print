package org.folio.print.server.service;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.print.server.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.Matchers.notNullValue;

@RunWith(VertxUnitRunner.class)
public class BatchCreationServiceTest extends TestBase {

  @Test
  public void createBatch() throws IOException {
    String message = getResourceAsString("mail/mail.json");

    JsonArray perm = new JsonArray().add("mod-batch-print.print.write").add("mod-batch-print.print.read");

    RestAssured.given()
            .header(XOkapiHeaders.TENANT, TENANT_1)
            .header(XOkapiHeaders.PERMISSIONS, perm.encode())
            .contentType(ContentType.JSON)
            .body(message)
            .post("/mail")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("id", notNullValue());

    RestAssured.given()
            .header(XOkapiHeaders.TENANT, TENANT_1)
            .header(XOkapiHeaders.PERMISSIONS, perm.encode())
            .contentType(ContentType.JSON)
            .body(message)
            .post("/mail")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("id", notNullValue());

    RestAssured.given()
            .baseUri(MODULE_URL)
            .header(XOkapiHeaders.TENANT, TENANT_1)
            .header(XOkapiHeaders.PERMISSIONS, perm.encode())
            .contentType(ContentType.JSON)
            .post("/print/batch-creation")
            .then()
            .statusCode(204);
  }

  private String getResourceAsString(String name) throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(name)) {
      if (inputStream == null) {
        throw new IOException("Resource not found: " + name);
      }
      return new String(inputStream.readAllBytes());
    }
  }
}
