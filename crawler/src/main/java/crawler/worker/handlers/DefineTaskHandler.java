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
 * This route handler handles the "/define-task" route on the worker side. It
 * receives new tasks set by users from master ("/submit-task" route) and
 * defines them.
 * 
 * @author YuanhongXiao
 *
 */
public class DefineTaskHandler implements Route {

	private static final Logger logger = LogManager.getLogger(DefineTaskHandler.class);

	private IWorkerStorage db = WorkerStorageSingleton.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		String startUrl = request.queryParams("startUrl");
		int totalNumOfDocumentsToCrawl = Integer.parseInt(request.queryParams("totalNumOfDocumentsToCrawl"));
		int totalNumOfCrawledDocuments = Integer.parseInt(request.queryParams("totalNumOfCrawledDocuments"));

		URLInfo startUrlInfo;

		try {
			startUrlInfo = new URLInfo(startUrl);
		} catch (MalformedURLException | URISyntaxException e) {
			logger.error("User submitted an invalid URL: " + startUrl, e);
			halt(HttpServletResponse.SC_BAD_REQUEST, "User submitted an invalid URL: " + startUrl);
			return "";
		}

		WorkerStatus.setTotalNumOfDocumentsToCrawl(totalNumOfDocumentsToCrawl);
		WorkerStatus.setTotalNumOfCrawledDocuments(totalNumOfCrawledDocuments);

		if (db.offerUrlIntoUrlQueue(startUrlInfo.toString())) {
			logger.info("Worker received a task to crawl documents starting from " + startUrlInfo.toString());
		} else {
			logger.info("Worker failed to offer the received start url " + startUrlInfo.toString());
		}

		return "";
	}

}
