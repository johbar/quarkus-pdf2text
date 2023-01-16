package johbar;


import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.jboss.logging.Logger;


import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletionException;

@WebServlet(urlPatterns = "/stream")
@Singleton
public class PdfToTextStreamServlet extends HttpServlet {

    public PdfToTextStreamServlet() {
        LOG.info("Servlet created");
    }




    final HttpClient httpClient = HttpClient.newBuilder().build();

    private static final Logger LOG = Logger.getLogger(PdfToTextStreamServlet.class);

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse resp)
            throws ServletException, IOException {
        var url = request.getParameter("url");
        URI uri = URI.create(url);
        var req = HttpRequest.newBuilder(uri).build();
        LOG.info("Start downloading of " + url);
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("text/plain; charset=UTF-8");
        final PDFTextStripper pts = new PDFTextStripper();
        try (var w = resp.getWriter()) {
            var doc = httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofInputStream())
                    .thenApply(HttpResponse::body)
                    .thenApply(this::loadPdf).get();
            LOG.info("Number of pages: " + doc.getNumberOfPages());
            pts.writeText(doc, w);
            w.flush();
        } catch (Exception e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }
    PDDocument loadPdf(InputStream is) {
        PDDocument doc;
        LOG.info("Opening PDF");
        try {
            doc = Loader.loadPDF(is, MemoryUsageSetting.setupMainMemoryOnly());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            throw new CompletionException(ex);
        }
        return doc;
    }
}
