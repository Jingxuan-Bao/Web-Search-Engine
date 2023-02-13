package crawler.worker.spouts;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.utils.URLInfo;
import crawler.worker.WorkerParameters;
import crawler.worker.WorkerStatus;
import crawler.worker.storage.IWorkerStorage;
import crawler.worker.storage.WorkerStorageSingleton;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * This is a spout in storm lite. It polls URLs from the URL queue and emits
 * them to DocumentFetcherBolt.
 * 
 * @author YuanhongXiao
 *
 */
public class CrawlerQueueSpout implements IRichSpout {

	private static final Logger logger = LogManager.getLogger(CrawlerQueueSpout.class);

	private String executorId = UUID.randomUUID().toString();
	// The collector is the destination for tuples; you "emit" tuples there
	private SpoutOutputCollector collector;
	private IWorkerStorage db;
	private boolean isWorking;

	public CrawlerQueueSpout() {
		logger.info("Starting CrawlerQueueSpout " + executorId);
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url", "hostName"));
	}

	@Override
	public void setRouter(IStreamRouter router) {
		collector.setRouter(router);
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;
		this.db = WorkerStorageSingleton.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());
	}

	@Override
	public void close() {
		if (WorkerStatus.shouldStopCrawling() && !isWorking) {
			WorkerStatus.kill(executorId);
			logger.info("CrawlerQueueSpout " + executorId + " is dead 1");
		}
	}

	@Override
	public void nextTuple() {
		if (!WorkerStatus.shouldStopCrawling()) {
			if (!WorkerStatus.isDone()) {
				logger.debug("CrawlerQueueSpout - " + WorkerStatus.getNumOfWorkingExecutors() + " working executors");
				WorkerStatus.startWorking();
				isWorking = true;
				pollNextUrlFromQueueAndEmit();
				isWorking = false;
				WorkerStatus.stopWorking();
				logger.debug("CrawlerQueueSpout - " + WorkerStatus.getNumOfWorkingExecutors() + " working executors");
			}
		} else {
			WorkerStatus.kill(executorId);
			logger.info("CrawlerQueueSpout " + executorId + " is dead 2");
		}
	}

	private void pollNextUrlFromQueueAndEmit() {
		String urlFromUrlQueue = db.pollUrlFromUrlQueue(); // may be null

		if (urlFromUrlQueue == null) {
			logger.info("CrawlerQueueSpout " + executorId + " failed to poll a URL because the queue is empty");
			return;
		}

		logger.info("The URL queue has " + db.getUrlQueueSize() + " URLs after polling!");

		URLInfo urlInfo;

		try {
			urlInfo = new URLInfo(urlFromUrlQueue);
		} catch (MalformedURLException | URISyntaxException e) {
			logger.error("URL: " + urlFromUrlQueue + " is NOT valid and is ignored by the CrawlerQueueSpout", e);
			return;
		}

		String url = urlInfo.toString();
		logger.info("CrawlerQueueSpout " + executorId + " polled a new url: " + url + " from the queue");
		WorkerStatus.startWorking(); // call this function for downstream bolts
		collector.emit(new Values<Object>(url, urlInfo.getHostName()));
		logger.info("CrawlerQueueSpout " + executorId + " emitted a new url: " + url);
	}

}