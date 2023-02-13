package crawler.master.handlers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.master.MasterServer;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/stop-crawling" route on the master side. It
 * sends stop crawling requests to every crawler worker to ask them to stop
 * crawling.
 * 
 * @author YuanhongXiao
 *
 */
public class MasterStopCrawlingHandler implements Route {

	private static final Logger logger = LogManager.getLogger(MasterStopCrawlingHandler.class);

	private List<String> workerKeys = MasterServer.getWorkerKeys();

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		for (String workerKey : workerKeys) {
			String worker = "http://" + workerKey + "/stop-crawling";
			try {
				URL workerUrl = new URL(worker);
				HttpURLConnection conn = (HttpURLConnection) workerUrl.openConnection();
				conn.setRequestMethod("GET");
				conn.getResponseCode();
				conn.disconnect();
			} catch (IOException e) {
				logger.error("Master failed to send the stop-crawling request to worker " + workerKey, e);
			}
		}

		response.redirect("/");

		return "";
	}

}
