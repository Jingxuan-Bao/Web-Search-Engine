package crawler.worker.bolts;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import crawler.utils.URLInfo;
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

/**
 * This is a bolt in storm lite. It receives crawled documents from
 * DocumentFetcherBolt and extract outgoing links. It emits nothing.
 * 
 * @author YuanhongXiao
 *
 */
public class LinkExtractorBolt implements IRichBolt {

	private static final Logger logger = LogManager.getLogger(LinkExtractorBolt.class);

	private Fields schema = new Fields(); // this bolt doesn't emit
	private String executorId = UUID.randomUUID().toString();
	// This is where we send our output stream
	private OutputCollector collector;
	private IWorkerStorage db;
	private int maxUrlLength;
	private boolean isWorking;

	public LinkExtractorBolt() {
		logger.info("Starting LinkExtractorBolt " + executorId);
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
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.db = WorkerStorageSingleton.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());
		this.maxUrlLength = WorkerParameters.getMaxUrlLength();
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
	public void cleanup() {
		if (WorkerStatus.shouldStopCrawling() && !isWorking) {
			WorkerStatus.kill(executorId);
			logger.info("LinkExtractorBolt " + executorId + " is dead 1");
		}
	}

	@Override
	public void execute(Tuple input) {
		if (!WorkerStatus.shouldStopCrawling()) {
			if (!WorkerStatus.isDone()) {
				logger.debug("LinkExtractorBolt - " + WorkerStatus.getNumOfWorkingExecutors() + " working executors");
				isWorking = true;
				extractAndEnqueueLinks(input);
				isWorking = false;
				WorkerStatus.stopWorking();
				logger.debug("LinkExtractorBolt - " + WorkerStatus.getNumOfWorkingExecutors() + " working executors");
			}
		} else {
			WorkerStatus.kill(executorId);
			logger.info("LinkExtractorBolt " + executorId + " is dead 2");
		}
	}

	private void extractAndEnqueueLinks(Tuple input) {
		// extract and enqueue links (only extract links from text/html files)
		// see piazza question @778
		String contentType = input.getStringByField("contentType");

		if (contentType == null || !contentType.equals("text/html")) {
			return;
		}

		String documentContent = input.getStringByField("documentContent");
		String urlFromDocumentFetcherBolt = input.getStringByField("url");

		URL currentUrl;

		try {
			currentUrl = new URL(urlFromDocumentFetcherBolt);
		} catch (MalformedURLException e) {
			logger.error("LinkExtractorBolt: " + executorId + " received an invalid URL from DocumentFetcherBolt", e);
			return;
		}

		// select all elements with attribute [href]
		// see piazza question @890
		Elements elements = Jsoup.parse(documentContent).select("[href]");

		for (Element element : elements) {
			try {
				String href = element.attr("href"); // href value

				if (!new URI(href).isAbsolute()) { // if link is a relative link
					href = new URL(currentUrl, href).toString();
				}

				URLInfo hrefInfo = new URLInfo(href); // for validity checking purpose
				String outgoingLink = hrefInfo.toString();

				if (outgoingLink.length() <= maxUrlLength) {
					// send outgoing link to master to distribute
					URL masterUrl = new URL("http://" + WorkerParameters.getMasterAddress()
							+ "/distribute-link?outgoingLink=" + outgoingLink);
					HttpURLConnection conn = (HttpURLConnection) masterUrl.openConnection();
					conn.setRequestMethod("POST");
					conn.setDoOutput(true);
					conn.getResponseCode();
					conn.disconnect();
					// add to storage
					db.addOutgoingLinkToDocument(urlFromDocumentFetcherBolt, outgoingLink);
				}
			} catch (IOException | URISyntaxException e) {
				logger.error("Failed to extract a valid outgoing link from the URL: " + urlFromDocumentFetcherBolt, e);
			}
		}
	}

}
