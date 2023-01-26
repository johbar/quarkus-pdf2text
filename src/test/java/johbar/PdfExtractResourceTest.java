package johbar;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;

@QuarkusTest
public class PdfExtractResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
          .when().get("/pdf?url=https://dserver.bundestag.de/btd/20/000/2000001.pdf")
          .then()
             .statusCode(200);
    }

}