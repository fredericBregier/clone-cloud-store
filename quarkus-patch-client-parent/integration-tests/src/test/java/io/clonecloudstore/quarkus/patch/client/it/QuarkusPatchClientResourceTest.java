package io.clonecloudstore.quarkus.patch.client.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class QuarkusPatchClientResourceTest {

  @Test
  public void testHelloEndpoint() {
    given().when().get("/quarkus-patch-client").then().statusCode(200).body(is("Hello quarkus-patch-client"));
  }
}
