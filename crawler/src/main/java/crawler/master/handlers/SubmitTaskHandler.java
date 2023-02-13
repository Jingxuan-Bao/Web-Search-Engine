package crawler.master.handlers;

import static spark.Spark.halt;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.master.MasterServer;
import crawler.utils.URLInfo;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/submit-task" route on the master side. When
 * users set a new crawling task by submitting the form on the home page, it
 * distributes the task to a worker based on the task's start URL's host name.
 * 
 * @author YuanhongXiao
 *
 */
public class SubmitTaskHandler implements Route {

	private static final Logger logger = LogManager.getLogger(SubmitTaskHandler.class);

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		String startUrl = request.queryParams("startUrl");
		int numOfDocumentsToCrawl = Integer.parseInt(request.queryParams("numOfDocumentsToCrawl"));

		MasterServer.addNumOfDocumentsToCrawl(numOfDocumentsToCrawl);

		URLInfo startUrlInfo;

		try {
			startUrlInfo = new URLInfo(startUrl);
		} catch (MalformedURLException | URISyntaxException e) {
			logger.error("User submitted an invalid URL: " + startUrl, e);
			halt(HttpServletResponse.SC_BAD_REQUEST, "User submitted an invalid URL: " + startUrl);
			return "";
		}

		String pickedWorkerKey = MasterServer.pickWorkerKey(startUrlInfo);
		String pickedWorker = "http://" + pickedWorkerKey + "/define-task?startUrl=" + startUrlInfo.toString()
				+ "&totalNumOfDocumentsToCrawl=" + MasterServer.getTotalNumOfDocumentsToCrawl()
				+ "&totalNumOfCrawledDocuments=" + MasterServer.getTotalNumOfCrawledDocuments();

		try {
			URL pickedWorkerUrl = new URL(pickedWorker);
			HttpURLConnection conn = (HttpURLConnection) pickedWorkerUrl.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.getResponseCode();
			conn.disconnect();
		} catch (IOException e) {
			logger.error("Master failed to send the submitted task {" + startUrlInfo.toString() + "} {"
					+ numOfDocumentsToCrawl + "} to worker " + pickedWorkerKey, e);
			halt(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Master failed to sent the submitted task {"
					+ startUrlInfo.toString() + "} {" + numOfDocumentsToCrawl + "} to worker " + pickedWorkerKey);
		}

		response.redirect("/");

		return "";
	}

}
