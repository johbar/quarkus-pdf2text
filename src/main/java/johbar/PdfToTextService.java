package johbar;

import io.quarkus.runtime.annotations.ConfigItem;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.util.DateConverter;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;

@Singleton
public class PdfToTextService {
    private static final Logger LOG = Logger.getLogger(PdfToTextService.class);

    @Inject
    ManagedExecutor executor;

    HttpClient httpClient;

    @ConfigItem(name = "pdfbox.max.memory.mebibytes")
    int maxMemory;

    @PostConstruct
    void init (){
        httpClient  = HttpClient.newBuilder().executor(executor).build();
        LOG.info("Number of Threads in ForkJoinPool: " + ForkJoinPool.getCommonPoolParallelism());
    }

    public CompletableFuture<String> remotePdfToText(URI uri) {
        return loadFromUriAsync(uri).thenApply(this::getTextAndClose);
    }

    public CompletableFuture<PDDocument> loadFromUriAsync(URI uri) {
        var req = HttpRequest.newBuilder(uri).build();
        LOG.info("Start downloading of " + uri);
        var doc = httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofInputStream())
                .thenApply(HttpResponse::body)
                .thenApply(this::loadPdf);
        return doc;
    }

    public PDDocument loadPdf(InputStream is) {
        PDDocument doc;
        LOG.info("Opening PDF");
        try {
            //doc = Loader.loadPDF(is, MemoryUsageSetting.setupMixed(maxMemory * 1024 * 1024));
            doc = Loader.loadPDF(is, MemoryUsageSetting.setupTempFileOnly());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            throw new CompletionException(ex);
        }
        return doc;
    }

    public String getTextAndClose(PDDocument doc) {
        String result;
        if (doc == null) {
            LOG.error("Document is null :(");
            return null;
        }
        LOG.info("Extracting Text...");
        var nPages = doc.getNumberOfPages();
        LOG.info("Number of pages: " + nPages);
        try (doc) {
            final PDFTextStripper pts = new PDFTextStripper();
            result = pts.getText(doc);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            throw new CompletionException(ex);
        }
        LOG.info("Text extracted. Length: " + result.length());
        return result.trim();
    }

    public Map<String, String> getTextAndMetadata(PDDocument doc) {
        return getTextAndMetadata(doc, "content");
    }

    public Map<String, String> getTextAndMetadata(PDDocument doc, String contentKey) {
        var result = new HashMap<String, String>();
        var info = doc.getDocumentInformation();
        var keys = info.getMetadataKeys();

        result.put("created", DateConverter.toISO8601(info.getCreationDate()));
        result.put("modified", DateConverter.toISO8601(info.getModificationDate()));

        keys.forEach((k) -> result.put(k, info.getCustomMetadataValue(k)));
        result.put(contentKey, getTextAndClose(doc));

        return result;
    }
}
