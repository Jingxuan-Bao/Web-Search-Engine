package crawler.master.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.master.MasterServer;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/confirm-stop-crawling" route on the master
 * side. It deals with workers confirmation on stop crawling request.
 * 
 * @author YuanhongXiao
 *
 */
public class ConfirmStopCrawlingHandler implements Route {

	private static final Logger logger = LogManager.getLogger(ConfirmStopCrawlingHandler.class);

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		String workerKey = request.ip() + ":" + request.queryParams("port"); // ip:port
		MasterServer.setWorkerHasStoppedCrawling(workerKey);

		logger.info("Master received the stop-crawling confirmation from worker " + workerKey);

		if (MasterServer.getNumOfCrawlingWorkers() == 0) {
			logger.info("Master finds that all workers have stopped crawling!");
			MasterServer.setWorkerWorkingPhase(MasterServer.PHASE.CrawlingStopped);
		}

		return "";
	}

}
