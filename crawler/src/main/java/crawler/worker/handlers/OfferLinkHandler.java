package crawler.worker.handlers;

import static spark.Spark.halt;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.utils.URLInfo;
import crawler.worker.WorkerParameters;
import crawler.worker.WorkerStatus;
import crawler.worker.storage.IWorkerStorage;
import crawler.worker.storage.WorkerStorageSingleton;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/offer-link" route on the worker side. It
 * receives redistributed outgoing links from master ("/distribute-link" route)
 * and offers it into the URL queue.
 * 
 * @author YuanhongXiao
 *
 */
public class OfferLinkHandler implements Route {

	private static final Logger logger = LogManager.getLogger(OfferLinkHandler.class);

	private IWorkerStorage db = WorkerStorageSingleton.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		String outgoingLink = request.queryParams("outgoingLink");
		int totalNumOfDocumentsToCrawl = Integer.parseInt(request.queryParams("totalNumOfDocumentsToCrawl"));
		int totalNumOfCrawledDocuments = Integer.parseInt(request.queryParams("totalNumOfCrawledDocuments"));

		URLInfo outgoingLinkInfo;

		try {
			outgoingLinkInfo = new URLInfo(outgoingLink);
		} catch (MalformedURLException | URISyntaxException e) {
			logger.error("Worker received an invalid outgoing link: " + outgoingLink, e);
			halt(HttpServletResponse.SC_BAD_REQUEST, "Worker received an invalid outgoing link: " + outgoingLink);
			return "";
		}

		WorkerStatus.setTotalNumOfDocumentsToCrawl(totalNumOfDocumentsToCrawl);
		WorkerStatus.setTotalNumOfCrawledDocuments(totalNumOfCrawledDocuments);

		if (db.offerUrlIntoUrlQueue(outgoingLinkInfo.toString())) {
			logger.info("Worker received and offered an outgoing link: " + outgoingLinkInfo.toString());
		} else {
			logger.info("Worker failed to offer an outgoing link: " + outgoingLinkInfo.toString());
		}

		return "";
	}

}
