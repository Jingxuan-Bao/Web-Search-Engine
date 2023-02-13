package crawler.worker.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.worker.WorkerParameters;
import crawler.worker.WorkerStatus;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/stop-crawling" route on the worker side. It
 * receives the stop crawling request from master ("/stop-crawling" route) and
 * changes worker status.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerStopCrawlingHandler implements Route {

	private static final Logger logger = LogManager.getLogger(WorkerStopCrawlingHandler.class);

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		WorkerStatus.stopCrawling();

		logger.info("Worker on port: " + WorkerParameters.getPort() + " received stop-crawling request from master");

		return "";
	}

}
