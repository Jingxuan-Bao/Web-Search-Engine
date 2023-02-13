package crawler.worker.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.utils.HeadRequestInfo;
import crawler.utils.RobotsTxtInfo;
import crawler.utils.URLInfo;
import crawler.worker.HostInfos;
import crawler.worker.WorkerParameters;
import crawler.worker.WorkerStatus;
import crawler.worker.storage.IWorkerStorage;
import crawler.worker.storage.WorkerStorageSingleton;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * This is a bolt in storm lite. It receives URLs from CrawlerQueueSpout,
 * fetches documents and emits fetched documents to LinkExtractorBolt.
 * 
 * @author YuanhongXiao
 *
 */
public class DocumentFetcherBolt implements IRichBolt {

	private static final Logger logger = LogManager.getLogger(DocumentFetcherBolt.class);

	private static final String CIS_455_CRAWLER = WorkerParameters.getUserAgent();

	private Fields schema = new Fields("documentContent", "contentType", "url");
	private String executorId = UUID.randomUUID().toString();
	// This is where we send our output stream
	private OutputCollector collector;
	private IWorkerStorage db;
	private int maxDocumentSize;
	private boolean isWorking;

	public DocumentFetcherBolt() {
		logger.info("Starting DocumentFetcherBolt " + executorId);
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.db = WorkerStorageSingleton.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());
		this.maxDocumentSize = WorkerParameters.getMaxDocumentSize();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public void cleanup() {
		if (WorkerStatus.shouldStopCrawling() && !isWorking) {
			WorkerStatus.kill(executorId);
			logger.info("DocumentFetcherBolt " + executorId + " is dead 1");
		}
	}

	@Override
	public void execute(Tuple input) {
		if (!WorkerStatus.shouldStopCrawling()) {
			if (!WorkerStatus.isDone()) {
				logger.debug("DocumentFetcherBolt - " + WorkerStatus.getNumOfWorkingExecutors() + " working executors");
				isWorking = true;
				fetchDocumentAndEmit(input);
				isWorking = false;
				WorkerStatus.stopWorking();
				logger.debug("DocumentFetcherBolt - " + WorkerStatus.getNumOfWorkingExecutors() + " working executors");
			}
		} else {
			WorkerStatus.kill(executorId);
			logger.info("DocumentFetcherBolt " + executorId + " is dead 2");
		}
	}

	private void fetchDocumentAndEmit(Tuple input) {
		String urlFromCrawlerQueueSpout = input.getStringByField("url");
		URLInfo urlInfo;

		try {
			urlInfo = new URLInfo(urlFromCrawlerQueueSpout);
		} catch (MalformedURLException | URISyntaxException e) {
			logger.error("URL: " + urlFromCrawlerQueueSpout + " is NOT valid and is ignored by the DocumentFetcherBolt",
					e);
			return;
		}

		String url = urlInfo.toString();
		logger.info("DocumentFetcherBolt " + executorId + " received a url: " + url);
		String hostName = urlInfo.getHostName();

		// get the robots.txt info
		RobotsTxtInfo robotsTxtInfo;

		if (HostInfos.hasRobotsTxtInfo(hostName)) { // have the robots.txt info of the host name
			robotsTxtInfo = HostInfos.getRobotsTxtInfo(hostName);
		} else { // don't have the robots.txt info of the host name
			try {
				robotsTxtInfo = new RobotsTxtInfo(urlInfo);
			} catch (NumberFormatException | IOException e) {
				logger.error("Failed to get the robots.txt info from the host: " + hostName);
				robotsTxtInfo = new RobotsTxtInfo();
			}
			HostInfos.setRobotsTxtInfo(hostName, robotsTxtInfo);
		}

		if (robotsTxtInfo.disallowPath(urlInfo.getPath())) { // robots.txt disallows the user agent to crawl
			logger.info("The robots.txt of the url: " + url + " disallows the cis455crawler to crawl");
			return;
		}

		// check Crawl-delay:
		// see piazza question @832
		ZonedDateTime hostLastCrawledTime = HostInfos.getLastCrawledTime(hostName); // may be null

		if (hostLastCrawledTime != null && hostLastCrawledTime.plusSeconds(robotsTxtInfo.getCrawlDelay())
				.isAfter(ZonedDateTime.now(ZoneId.of("GMT")))) { // can't crawl now
			logger.info("The cis455crawler has to wait to crawl the url: " + url + " which has a Crawl-delay of "
					+ robotsTxtInfo.getCrawlDelay() + " seconds");
			db.offerUrlIntoUrlQueue(url); // put the URL back to the queue
			return;
		}

		// always send a HEAD request first
		HeadRequestInfo headRequestInfo;

		try {
			headRequestInfo = new HeadRequestInfo(urlInfo, db);
		} catch (IOException e) {
			logger.error("Failed to send a HEAD request to the url: " + url, e);
			return;
		}

		String contentType = headRequestInfo.getContentType();
		long contentLength = headRequestInfo.getContentLength(); // in bytes
		boolean isModified = headRequestInfo.isModified();

		// check file type and file size
		if (!contentType.equals("text/html") && !contentType.equals("application/xml")
				&& !contentType.equals("text/xml") && !contentType.endsWith("+xml")) { // file has wrong Content-Type
			logger.info("URL: " + url + " has a wrong Content-Type: " + contentType);
			return;
		}
		if (contentLength < 0 || contentLength > maxDocumentSize * 1000000) {
			// file is greater than the specified max size
			logger.info("URL: : " + url + " has an invalid Content-Length: " + contentLength);
			return;
		}

		// crawl
		// two cases: the URL has been crawled; the URL has not been crawled
		String documentContent = "";

		if (db.hasCrawledDocument(url)) { // case 1: the URL has been crawled
			logger.info("URL: " + url + " has already been crawled");
			if (isModified) { // file has been modified since the last time it was crawled
				logger.info("URL: " + url + " has been modified since the last time it was crawled");
				try {
					documentContent = downloadPage(urlInfo);
					logger.info(url + ": downloading"); // per homework handout
					// update host name's last crawled time
					HostInfos.updateLastCrawledTime(hostName);
					// index the document
					db.addDocument(url, documentContent, contentType, contentLength);
					// add document hash to content-seen database
					db.addDocumentContentHash(getMD5Hash(documentContent), url);
				} catch (IOException e) {
					logger.error("Failed to download the content of the URL: " + url, e);
					return;
				}
			} else { // file has not been modified since the last time it was crawled
				logger.info(url + ": not modified"); // per homework handout
				documentContent = db.getDocumentContent(url);
			}
		} else { // case 2: the URL has not been crawled
			logger.info("URL: " + url + " has not been crawled before");
			try {
				documentContent = downloadPage(urlInfo);
				logger.info(url + ": downloading"); // per homework handout
				// update host name's last crawled time
				HostInfos.updateLastCrawledTime(hostName);
				String documentContentHash = getMD5Hash(documentContent);
				if (db.hasDocumentContentHash(documentContentHash)) {
					// has crawled a document with a matching hash
					logger.info("Ignore the URL: " + url + " because of a matching content hash");
					// drop this URL and do nothing
					// see piazza question @819
					return;
				}
				// index the document
				db.addDocument(url, documentContent, contentType, contentLength);
				// add document hash to content-seen database
				db.addDocumentContentHash(documentContentHash, url);
			} catch (IOException e) {
				logger.error("Failed to download the content of the URL: " + url, e);
				return;
			}
		}

		logger.info(db.getCorpusSize() + " URLs have been indexed!");
		WorkerStatus.startWorking(); // call this function for downstream bolts
		// emit document
		collector.emit(new Values<Object>(documentContent, contentType, url));
	}

	private String downloadPage(URLInfo urlInfo) throws IOException {
		URL url = new URL(urlInfo.toString());
		BufferedReader br;

		if (urlInfo.isSecure()) { // https
			HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
			conn.setRequestProperty("User-Agent", CIS_455_CRAWLER);
			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		} else { // http
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestProperty("User-Agent", CIS_455_CRAWLER);
			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		}

		StringBuilder res = new StringBuilder();
		String line = null;

		while ((line = br.readLine()) != null) {
			res.append(line + "\n");
		}

		return res.toString();
	}

	private String getMD5Hash(String documentContent) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] byteDigest = md.digest(documentContent.getBytes("UTF-8"));
			return new String(byteDigest, "UTF-8");
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			logger.error("Failed to get an MD5 hash, return raw document content");
			return documentContent;
		}
	}

}
