package crawler.worker.storage;

/**
 * This class is a singleton class for the local Berkeley DB storage on disk.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerStorageSingleton {

	private static IWorkerStorage db = null;

	public static IWorkerStorage getWorkerStorageInstance(String storageDirectory) {
		if (db == null) {
			db = new WorkerStorage(storageDirectory);
		}
		return db;
	}

}
