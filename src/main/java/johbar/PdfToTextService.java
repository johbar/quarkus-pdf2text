package johbar;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import jakarta.annotation.PostConstruct;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.util.DateConverter;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.ConfigItem;

@Singleton
public class PdfToTextService {
    private static final Logger LOG = Logger.getLogger(PdfToTextService.class);

    @Inject
    ManagedExecutor executor;

    HttpClient httpClient;

    @ConfigItem(name = "pdfbox.max.memory.mebibytes")
    int maxMemory;

    boolean dehypenationEnabled = true;

    @PostConstruct
    void init() {
        this.httpClient = HttpClient.newBuilder().executor(this.executor).build();
        LOG.info("Number of Threads in ForkJoinPool: " + ForkJoinPool.getCommonPoolParallelism());

    }

    public CompletableFuture<String> remotePdfToText(final URI uri) {
        return this
                .loadFromUriAsync(uri)
                .thenComposeAsync(this::getTextAndClose, this.executor);
    }

    public CompletableFuture<InputStream> loadStream(final URI uri) {
        final var req = HttpRequest.newBuilder(uri).build();
        LOG.info("Start downloading of " + uri);
        return this.httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.ofInputStream())
                .thenApplyAsync(HttpResponse::body, this.executor);
    }

    public CompletableFuture<PDDocument> loadFromUriAsync(final URI uri) {
        final var doc = this.loadStream(uri)
                .thenApplyAsync(this::loadPdf, this.executor);
        return doc;
    }

    public PDDocument loadPdf(final InputStream is) {
        PDDocument doc;
        LOG.info("Opening PDF");
        try {
            doc = Loader.loadPDF(is, MemoryUsageSetting.setupMixed(this.maxMemory * 1024 * 1024));
            // doc = Loader.loadPDF(is, MemoryUsageSetting.setupTempFileOnly());
        } catch (final Exception ex) {
            LOG.error(ex.getMessage());
            throw new CompletionException(ex);
        }
        return doc;
    }

    public CompletableFuture<String> getTextAndClose(final PDDocument doc) {
        if (doc == null) {
            LOG.error("Document is null :(");
            return null;
        }
        LOG.info("Extracting Text...");
        final var nPages = doc.getNumberOfPages();
        LOG.info("Number of pages: " + nPages);
        final PDFTextStripper pts = new PDFTextStripper();
        final var os = new PipedOutputStream();
        final var ow = new OutputStreamWriter(os);

        CompletableFuture<String> result = this.executor.supplyAsync(() -> this.dehyphenate(os));
        try (doc; ow) {
            LOG.debug("TextStripper started");
            pts.writeText(doc, ow);
            LOG.debug("TextStripper finished");
        } catch (final IOException e) {
            LOG.error(e);
        }
        return result;
    }

    String dehyphenate (PipedOutputStream os) {
        final var sb = new StringBuilder();
        
        try {
            final var is = new PipedInputStream(os);
            final var isr = new InputStreamReader(is);
            final var br = new BufferedReader(isr);
            String l1 = br.readLine();
            LOG.info("First line read for dehyphenation");
            while (true) {
                // read one line
                if (null == l1) {
                    // LOG.debug("EOF");
                    break; //EOF
                }
                l1 = l1.strip();
                if (l1.isBlank()) {
                    l1 = br.readLine();
                    continue;
                }
                if (l1.endsWith("-") && this.dehypenationEnabled) {
                    // the line is a dehyphenation candidate
                    // read another line to verify:
                    var l2 = br.readLine();
                    if (null == l2) {
                        sb.append(l1);
                        // LOG.debug("EOF");
                        break; //EOF
                    }
                    l2 = l2.strip();
                    if (Character.isLowerCase(l2.charAt(0))) {
                        // if the next line starts with a lower case, join it with its predecessor
                        l1 = l1.replaceFirst("-$", l2);
                    } else {
                        // if not, join the first line including the trailing dash with the second

                        l1 = l1.concat(l2);
                    }
                } else {
                    // normal line ending
                    // join lines with space in between
                    sb.append(l1).append(" ");
                    l1 = br.readLine();
                }
            }
            br.close();
        } catch (IOException ex) {
            LOG.error(ex);
        }
        var str = sb.toString();
        LOG.info("Dehyphenation finished. " + LocalDateTime.now());
        return str;
    }

    public Map<String, String> getTextAndMetadata(final PDDocument doc) {
        return this.getTextAndMetadata(doc, "content");
    }

    public Map<String, String> getTextAndMetadata(final PDDocument doc, final String contentKey)  {
        final var result = new HashMap<String, String>();
        final var info = doc.getDocumentInformation();
        final var keys = info.getMetadataKeys();

        result.put("created", DateConverter.toISO8601(info.getCreationDate()));
        result.put("modified", DateConverter.toISO8601(info.getModificationDate()));

        keys.forEach((k) -> result.put(k, info.getCustomMetadataValue(k)));
        String content;
        try {
            content = this.getTextAndClose(doc).get();
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error(ex);
            content = "";
        }
        result.put(contentKey, content);

        return result;
    }

    /**
     * Stream a PDF right to poppler-utils' pdftotext CLI and return the plain text
     * content.
     * 
     * @param is
     * @return
     */
    public CompletableFuture<String> pdfToText(final InputStream is) {
        final var pb = new ProcessBuilder("pdftotext", "-nopgbrk", "-", "-");
        try {
            final var proc = pb.start();
            final var stdin = proc.getOutputStream();
            LOG.info("pdftotext started");
            is.transferTo(stdin);
            stdin.close();
            LOG.info("File fed to pdftotext");
            final var stdout = proc.getInputStream();
            final var isr = new InputStreamReader(stdout, StandardCharsets.UTF_8);
            final var br = new BufferedReader(isr);
            final var result = br.lines()
                    .filter(l -> !l.isBlank())
                    .collect(Collectors.joining("\n"));
            LOG.info("Output consumed from pdftotext");
            return proc.onExit().thenApply(p -> {
                return result;
            });
        } catch (final IOException e) {
            LOG.error(e);
            return CompletableFuture.failedFuture(e);
        }
    }
}
