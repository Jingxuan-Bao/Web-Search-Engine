package crawler.master.handlers;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.master.MasterServer;
import crawler.master.WorkerInfo;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/worker-info" route on the master side. It
 * receives workers' periodical report of their status and makes update on the
 * master side.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerInfoHandler implements Route {

	private static final Logger logger = LogManager.getLogger(WorkerInfoHandler.class);

	private List<String> workerKeys = MasterServer.getWorkerKeys();
	private Map<String, WorkerInfo> workerInfos = MasterServer.getWorkerInfos();

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/plain");

		String ip = request.ip();
		int port = Integer.parseInt(request.queryParams("port"));
		int numOfIndexedDocuments = Integer.parseInt(request.queryParams("numOfIndexedDocuments"));
		int numOfQueuingUrls = Integer.parseInt(request.queryParams("numOfQueuingUrls"));
		int numOfUploadedDocuments = Integer.parseInt(request.queryParams("numOfUploadedDocuments"));
		boolean hasStoppedCrawling = Boolean.parseBoolean(request.queryParams("hasStoppedCrawling"))
				|| numOfUploadedDocuments > 0;

		// workers' working phase
		if (MasterServer.getWorkerWorkingPhase() == MasterServer.PHASE.Waiting && numOfIndexedDocuments > 0) {
			MasterServer.setWorkerWorkingPhase(MasterServer.PHASE.Crawling);
		} else if (numOfUploadedDocuments > 0) {
			MasterServer.setWorkerWorkingPhase(MasterServer.PHASE.Uploading);
		}

		String workerKey = ip + ":" + port;

		if (!workerInfos.containsKey(workerKey)) {
			workerKeys.add(workerKey);
			logger.info("Master added a new worker " + workerKey);
		}

		workerInfos.put(workerKey, new WorkerInfo(ip, port, numOfIndexedDocuments, numOfQueuingUrls,
				numOfUploadedDocuments, hasStoppedCrawling));

		logger.info("Master received and updated worker crawl info from " + workerKey);

		return "";
	}

}
