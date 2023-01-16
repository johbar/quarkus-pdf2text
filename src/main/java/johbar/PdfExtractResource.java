package johbar;


import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestQuery;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Path("/pdf")
public class PdfExtractResource {

    private static final Logger LOG = Logger.getLogger(PdfExtractResource.class);

    @Inject
    PdfToTextService service;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public CompletableFuture<String> extractPlain(@RestQuery URI url) {
        LOG.infof("Requested to extract %s.", url);
        return service.remotePdfToText(url);
    }

    @GET
    @Path("forget")
    public void fireAndForget(@RestQuery URI url) {
        service.remotePdfToText(url);
    }

    @Path("json")
    @Produces("application/json")
    @GET
    public CompletableFuture<Map<String, String>> getJson(@RestQuery URI url) {
        return service
                .loadFromUriAsync(url)
                .thenApply(service::getTextAndMetadata);
    }

}
