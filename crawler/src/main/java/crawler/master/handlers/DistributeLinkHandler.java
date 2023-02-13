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
 * This route handler handles the "/distribute-link" route on the master side.
 * It receives workers' extracted outgoing links and redistributes them to
 * different workers based on each link's host name.
 * 
 * @author YuanhongXiao
 *
 */
public class DistributeLinkHandler implements Route {

	private static final Logger logger = LogManager.getLogger(DistributeLinkHandler.class);

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		String outgoingLink = request.queryParams("outgoingLink");
		URLInfo outgoingLinkInfo;

		try {
			outgoingLinkInfo = new URLInfo(outgoingLink);
		} catch (MalformedURLException | URISyntaxException e) {
			logger.error("Master received an invalid outgoing link: " + outgoingLink, e);
			halt(HttpServletResponse.SC_BAD_REQUEST, "Master received an invalid outgoing link: " + outgoingLink);
			return "";
		}

		String pickedWorkerKey = MasterServer.pickWorkerKey(outgoingLinkInfo);
		String pickedWorker = "http://" + pickedWorkerKey + "/offer-link?outgoingLink=" + outgoingLinkInfo.toString()
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
			logger.error("Master failed to send the received outgoing link {" + outgoingLinkInfo.toString()
					+ "} to worker " + pickedWorkerKey, e);
			halt(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Master failed to send the received outgoing link {"
					+ outgoingLinkInfo.toString() + "} to worker " + pickedWorkerKey);
		}

		return "";
	}

}
