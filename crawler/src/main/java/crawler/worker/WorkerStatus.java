package crawler.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import crawler.worker.storage.IWorkerStorage;
import crawler.worker.storage.WorkerStorageSingleton;

/**
 * This class is used by the crawler worker to keep its working status.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerStatus {

	private static IWorkerStorage db = WorkerStorageSingleton
			.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());

	// learn these two numbers from the master
	private static AtomicInteger totalNumOfDocumentsToCrawl = new AtomicInteger(0);
	private static AtomicInteger totalNumOfCrawledDocuments = new AtomicInteger(0);
	// include both spouts and bolts
	private static AtomicInteger numOfWorkingExecutors = new AtomicInteger(0);
	// number of dead executors (spouts and bolts)
	// ConcurrentMap<executor id, 1>
	private static ConcurrentMap<String, Integer> deadExecutorsMap = new ConcurrentHashMap<String, Integer>();
	// know from master
	private static AtomicBoolean shouldStopCrawling = new AtomicBoolean();
	// get from my own storage
	private static int numOfUploadedDocuments;

	public static int getTotalNumOfDocumentsToCrawl() {
		return totalNumOfDocumentsToCrawl.get();
	}

	public static void setTotalNumOfDocumentsToCrawl(int totalNumOfDocumentsToCrawl) {
		WorkerStatus.totalNumOfDocumentsToCrawl.set(totalNumOfDocumentsToCrawl);
	}

	public static int getTotalNumOfCrawledDocuments() {
		return totalNumOfCrawledDocuments.get();
	}

	public static void setTotalNumOfCrawledDocuments(int totalNumOfCrawledDocuments) {
		WorkerStatus.totalNumOfCrawledDocuments.set(totalNumOfCrawledDocuments);
	}

	public static int getNumOfWorkingExecutors() {
		return numOfWorkingExecutors.get();
	}

	public static void startWorking() {
		numOfWorkingExecutors.incrementAndGet();
	}

	public static void stopWorking() {
		numOfWorkingExecutors.decrementAndGet();
	}

	public static int getNumOfDeadExecutors() {
		return deadExecutorsMap.size();
	}

	public static void kill(String executorId) {
		deadExecutorsMap.put(executorId, 1);
	}

	public static void stopCrawling() {
		shouldStopCrawling.set(true);
	}

	public static boolean shouldStopCrawling() {
		return shouldStopCrawling.get();
	}

	public static void incrementNumOfUploadedDocuments() {
		numOfUploadedDocuments++;
	}

	public static int getNumOfUploadedDocuments() {
		return numOfUploadedDocuments;
	}

	public static boolean isDone() {
		return totalNumOfCrawledDocuments.get() >= totalNumOfDocumentsToCrawl.get()
				|| (numOfWorkingExecutors.get() == 0 && db.urlQueueIsEmpty());
	}

}
