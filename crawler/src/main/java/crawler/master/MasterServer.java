package crawler.master;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.master.handlers.ConfirmStopCrawlingHandler;
import crawler.master.handlers.DistributeLinkHandler;
import crawler.master.handlers.HomeHandler;
import crawler.master.handlers.MasterStopCrawlingHandler;
import crawler.master.handlers.SubmitTaskHandler;
import crawler.master.handlers.WorkerInfoHandler;
import crawler.utils.URLInfo;

/**
 * This is the crawler master server which allows users to set new crawling
 * tasks, manages distributed crawler workers and displays crawling information.
 * 
 * @author YuanhongXiao
 *
 */
public class MasterServer {

	private static final Logger logger = LogManager.getLogger(MasterServer.class);

	// worker's key = ip:port
	private static List<String> workerKeys = new ArrayList<String>();
	// Map<worker's key, workerInfo object>
	private static Map<String, WorkerInfo> workerInfos = new HashMap<String, WorkerInfo>();
	// total number of documents to crawl
	private static int totalNumOfDocumentsToCrawl;

	// "Waiting" -> "Crawling" -> "Crawling Stopped" -> "Uploading to AWS"
	public static enum PHASE {
		Waiting, Crawling, CrawlingStopped, Uploading
	};

	private static PHASE workerWorkingPhase = PHASE.Waiting;

	/**
	 * Determine the worker to crawl the given URL by URL's host name
	 * 
	 * @param urlInfo
	 * @return Picked worker's key (ip:port)
	 */
	public static String pickWorkerKey(URLInfo urlInfo) {
		return workerKeys.get(urlInfo.getHostName().hashCode() % workerKeys.size());
	}

	public static List<String> getWorkerKeys() {
		return workerKeys;
	}

	public static Map<String, WorkerInfo> getWorkerInfos() {
		return workerInfos;
	}

	public static void setWorkerHasStoppedCrawling(String workerKey) {
		if (workerInfos.containsKey(workerKey)) {
			workerInfos.get(workerKey).setHasStoppedCrawling(true);
		}
	}

	public static int getNumOfCrawlingWorkers() {
		int res = 0;
		for (WorkerInfo workerInfo : workerInfos.values()) {
			if (!workerInfo.hasStoppedCrawling()) {
				res++;
			}
		}
		return res;
	}

	public static void addNumOfDocumentsToCrawl(int numOfDocumentsToCrawl) {
		totalNumOfDocumentsToCrawl += numOfDocumentsToCrawl;
	}

	public static int getTotalNumOfDocumentsToCrawl() {
		return totalNumOfDocumentsToCrawl;
	}

	public static int getTotalNumOfCrawledDocuments() {
		int res = 0;
		for (WorkerInfo workerInfo : workerInfos.values()) {
			res += workerInfo.getNumOfIndexedDocuments();
		}
		return res;
	}

	public static int getTotalNumOfUploadedDocuments() {
		int res = 0;
		for (WorkerInfo workerInfo : workerInfos.values()) {
			res += workerInfo.getNumOfUploadedDocuments();
		}
		return res;
	}

	public static PHASE getWorkerWorkingPhase() {
		return workerWorkingPhase;
	}

	public static void setWorkerWorkingPhase(PHASE workerWorkingPhase) {
		MasterServer.workerWorkingPhase = workerWorkingPhase;
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Syntax: Crawler Master {My Port Number}");
			System.exit(1);
		}

		int myPortNumber = Integer.parseInt(args[0]);

		port(myPortNumber);

		logger.info("Master is starting up on port: " + myPortNumber);

		// set REST routes with handlers
		// GET /, /home
		get("/", new HomeHandler());
		get("/home", new HomeHandler());
		// POST /confirm-stop-crawling?port=...
		post("/confirm-stop-crawling", new ConfirmStopCrawlingHandler());
		// POST /distribute-link?outgoingLink=...
		post("/distribute-link", new DistributeLinkHandler());
		// GET /stop-crawling
		get("/stop-crawling", new MasterStopCrawlingHandler());
		// POST /submit-task?startUrl=...&numOfDocumentsToCrawl=...
		post("/submit-task", new SubmitTaskHandler());
		// POST
		// /worker-info?port=...&numOfIndexedDocuments=...&numOfQueuingUrls=...&numOfUploadedDocuments=...&hasStoppedCrawling=...
		post("/worker-info", new WorkerInfoHandler());
	}

}
