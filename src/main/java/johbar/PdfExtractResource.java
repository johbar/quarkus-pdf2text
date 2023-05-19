package johbar;


import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestQuery;

@Path("/pdf")
public class PdfExtractResource {

    private static final Logger LOG = Logger.getLogger(PdfExtractResource.class);

    @Inject
    PdfToTextService service;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public CompletableFuture<String> extractPlain(@RestQuery URI url) {
        LOG.info("Requested to extract " + url);
        return this.service.remotePdfToText(url);
    }

    @GET
    @Path("forget")
    public void fireAndForget(@RestQuery URI url) {
        this.service.remotePdfToText(url);
    }

    @Path("json")
    @Produces(MediaType.APPLICATION_JSON)
    @GET
    public CompletableFuture<Map<String, String>> getJson(@RestQuery URI url) {
        return this.service
                .loadFromUriAsync(url)
                .thenApply(this.service::getTextAndMetadata);
    }


    @Path("large")
    @Produces(MediaType.TEXT_PLAIN)
    @GET
    public CompletableFuture<String> getLargeText(@RestQuery URI url) throws IOException, InterruptedException, ExecutionException {
        return this.service.loadStream(url).thenApplyAsync(this.service::pdfToText).get();

    }
}
