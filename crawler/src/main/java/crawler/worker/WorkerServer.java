package crawler.worker;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.aws.RDS;
import crawler.worker.bolts.DocumentFetcherBolt;
import crawler.worker.bolts.LinkExtractorBolt;
import crawler.worker.handlers.DefineTaskHandler;
import crawler.worker.handlers.OfferLinkHandler;
import crawler.worker.handlers.WorkerStopCrawlingHandler;
import crawler.worker.spouts.CrawlerQueueSpout;
import crawler.worker.storage.IWorkerStorage;
import crawler.worker.storage.WorkerStorageSingleton;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;

/**
 * This is the crawler worker server which can perform both crawling web
 * documents and uploading crawled web documents on disk to the remote AWS RDS.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerServer {

	private static final Logger logger = LogManager.getLogger(WorkerServer.class);

	private static IWorkerStorage db;

	private static final int REPORT_TO_MASTER_PERIOD = 10; // time in seconds

	private static void crawlAndIndex(int crawlerQueueSpoutParallelism, int documentFetcherBoltParallelism,
			int linkExtractorBoltParallelism) {
		final String CRAWLER_QUEUE_SPOUT = "crawler_queue_spout";
		final String DOCUMENT_FETCHER_BOLT = "document_fetcher_bolt";
		final String LINK_EXTRACTOR_BOLT = "link_extractor_bolt";

		// IMPORTANT
		// stormlite seems to have a bug: stormlite doesn't stop the Spout executors
		// Thus, number of Spout executors is not included here
		final int NUM_OF_EXECTORS = documentFetcherBoltParallelism + linkExtractorBoltParallelism;

		Config config = new Config();

		CrawlerQueueSpout crawlerQueueSpout = new CrawlerQueueSpout();
		DocumentFetcherBolt documentFetcherBolt = new DocumentFetcherBolt();
		LinkExtractorBolt linkExtractorBolt = new LinkExtractorBolt();

		TopologyBuilder builder = new TopologyBuilder();

		// CrawlerQueueSpout
		builder.setSpout(CRAWLER_QUEUE_SPOUT, crawlerQueueSpout, crawlerQueueSpoutParallelism);
		// DocumentFetcherBolt
		builder.setBolt(DOCUMENT_FETCHER_BOLT, documentFetcherBolt, documentFetcherBoltParallelism)
				.fieldsGrouping(CRAWLER_QUEUE_SPOUT, new Fields("hostName"));
		// LinkExtractorBolt
		builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, linkExtractorBoltParallelism)
				.shuffleGrouping(DOCUMENT_FETCHER_BOLT);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("crawler", config, builder.createTopology()); // run

		while (!WorkerStatus.shouldStopCrawling()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error("Crawler loop Thread.sleep() error", e);
			}
		}

		logger.info("Crawler is ready to STOP CRAWLING!");

		cluster.shutdown();

		while (WorkerStatus.getNumOfDeadExecutors() < NUM_OF_EXECTORS) {
			logger.info("NUMBER OF DEAD EXECUTORS --- " + WorkerStatus.getNumOfDeadExecutors());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error("Crawler loop Thread.sleep() error", e);
			}
		}

		logger.info("All executors are DEAD!");

		cluster.killTopology("crawler");
	}

	/**
	 * Periodically report worker status to master /worker-info
	 * 
	 * @param period time in seconds
	 */
	private static void sendWorkerInfoPeriodically(int period) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				sendWorkerInfo();
			}
		};

		new Timer().scheduleAtFixedRate(task, 0, period * 1000);
	}

	private static void sendWorkerInfo() {
		StringBuilder urlSb = new StringBuilder("http://" + WorkerParameters.getMasterAddress() + "/worker-info");
		urlSb.append("?port=" + WorkerParameters.getPort());
		urlSb.append("&numOfIndexedDocuments=" + db.getCorpusSize());
		urlSb.append("&numOfQueuingUrls=" + db.getUrlQueueSize());
		urlSb.append("&numOfUploadedDocuments=" + WorkerStatus.getNumOfUploadedDocuments());
		urlSb.append("&hasStoppedCrawling=" + WorkerStatus.shouldStopCrawling());

		try {
			URL url = new URL(urlSb.toString());
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.getResponseCode();
			conn.disconnect();
		} catch (IOException e) {
			logger.error("Worker on port " + WorkerParameters.getPort() + " failed to report worker info", e);
		}
	}

	/**
	 * Send shutdown confirmation to master
	 */
	private static void sendConfirmStopCrawling() {
		try {
			URL url = new URL("http://" + WorkerParameters.getMasterAddress() + "/confirm-stop-crawling?port="
					+ WorkerParameters.getPort());
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.getResponseCode();
			conn.disconnect();
		} catch (IOException e) {
			logger.error("Worker on port " + WorkerParameters.getPort() + " failed to send stop-crawling confirmation",
					e);
		}
	}

	public static void main(String[] args) {
		//////////////////// Uploading ////////////////////

		if (args.length == 8) {
			// {Master Server Address} {My Port Number} {Storage Directory}
			// {RDS DB Name} {RDS DB Username} {RDS DB Password}
			// {RDS DB Documents Table Name} {RDS DB Links Table Name}

			logger.info("Crawler worker is starting for uploading!");

			int myPortNumber = Integer.parseInt(args[1]);
			String storageDirectory = args[2];

			if (!Files.exists(Paths.get(storageDirectory))) {
				logger.debug("Worker on port " + myPortNumber + " found NO storage directory for uploading!");
				System.exit(1);
			}

			// set worker parameters
			WorkerParameters.setMasterAddress(args[0]);
			WorkerParameters.setPort(myPortNumber);
			WorkerParameters.setStorageDirectory(storageDirectory);
			WorkerParameters.setRdsDbName(args[3]);
			WorkerParameters.setRdsDbUsername(args[4]);
			WorkerParameters.setRdsDbPassword(args[5]);
			WorkerParameters.setRdsDbDocumentsTableName(args[6]);
			WorkerParameters.setRdsDbLinksTableName(args[7]);

			// get a worker storage instance (singleton)
			db = WorkerStorageSingleton.getWorkerStorageInstance(storageDirectory);

			try {
				RDS.connect();
			} catch (ClassNotFoundException | SQLException e) {
				logger.error("Worker on port " + myPortNumber + " failed to connect to AWS RDS", e);
				System.exit(1);
			}

			port(myPortNumber);
			logger.info("Worker is starting up on port: " + myPortNumber);

			sendWorkerInfoPeriodically(REPORT_TO_MASTER_PERIOD);

			RDS.upload();

			try {
				RDS.close();
			} catch (SQLException e) {
				logger.error("Worker on port " + myPortNumber + " failed to close AWS RDS", e);
				System.exit(1);
			}

			sendWorkerInfo();

			db.close();

			System.exit(0);
		}

		//////////////////// Crawling ////////////////////

		if (args.length != 7) {
			System.out.println(
					"Syntax: Crawler Worker {Master Server Address} {My Port Number} {Storage Directory} {Max Document Size in MB} "
							+ "{Max URL Length} {Number of DocumentFetcherBolt Executors} {Number of LinkExtractorBolt Executors}");
			System.exit(1);
		}

		logger.info("Crawler worker is starting for crawling!");

		int myPortNumber = Integer.parseInt(args[1]);
		String storageDirectory = args[2];

		// create worker storage directory
		if (!Files.exists(Paths.get(storageDirectory))) {
			try {
				Files.createDirectory(Paths.get(storageDirectory));
			} catch (IOException e) {
				logger.error("Failed to create the directory: " + Paths.get(storageDirectory), e);
				System.exit(1);
			}
		}

		// set worker parameters
		WorkerParameters.setMasterAddress(args[0]);
		WorkerParameters.setPort(myPortNumber);
		WorkerParameters.setStorageDirectory(storageDirectory);
		WorkerParameters.setMaxDocumentSize(Integer.parseInt(args[3]));
		WorkerParameters.setMaxUrlLength(Integer.parseInt(args[4]));

		// create a worker storage instance (singleton)
		db = WorkerStorageSingleton.getWorkerStorageInstance(storageDirectory);

		port(myPortNumber);
		logger.info("Worker is starting up on port: " + myPortNumber);

		// set REST routes with handlers
		// POST
		// /define-task?startUrl=...&totalNumOfDocumentsToCrawl=...&totalNumOfCrawledDocuments=...
		post("/define-task", new DefineTaskHandler());
		// POST
		// /offer-link?outgoingLink=...&totalNumOfDocumentsToCrawl=...&totalNumOfCrawledDocuments=...
		post("/offer-link", new OfferLinkHandler());
		// GET /stop-crawling
		get("/stop-crawling", new WorkerStopCrawlingHandler());

		sendWorkerInfoPeriodically(REPORT_TO_MASTER_PERIOD);

		crawlAndIndex(1, Integer.parseInt(args[5]), Integer.parseInt(args[6]));

		sendWorkerInfo();

		sendConfirmStopCrawling();

		db.close();

		System.exit(0);
	}

}
