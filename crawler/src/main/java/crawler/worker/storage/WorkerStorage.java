package crawler.worker.storage;

import java.io.File;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * This class implements the IWorkerStorage interface.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerStorage implements IWorkerStorage {

	private static final String URL_QUEUE_MAP_KEY = "url_queue_map_key";

	// database environment
	private Environment env;
	// Java class catalog
	private static final String CLASS_CATALOG = "java_class_catalog";
	private StoredClassCatalog javaCatalog;
	// databases
	private static final String DOCUMENT_STORE = "document_store";
	private static final String URL_QUEUE_STORE = "url_queue_store";
	private static final String CONTENT_SEEN_STORE = "content_seen_store";
	private Database documentDb;
	private Database urlQueueDb;
	private Database contentSeenDb;
	// bindings and collections
	private StoredSortedMap<String, Document> documentMap; // StoredSortedMap<URL, Document object>
	private StoredSortedMap<String, UrlQueue> urlQueueMap; // StoredSortedMap<default key, UrlQueue object>
	private StoredSortedMap<String, String> contentSeenMap; // StoredSortedMap<document content MD5 hash, URL>

	public WorkerStorage(String storageDirectory) {
		// open the database environment
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(true);
		envConfig.setAllowCreate(true);
		env = new Environment(new File(storageDirectory), envConfig);
		// open the Java class catalog
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(true);
		dbConfig.setAllowCreate(true);
		Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
		javaCatalog = new StoredClassCatalog(catalogDb);
		// open databases
		documentDb = env.openDatabase(null, DOCUMENT_STORE, dbConfig);
		urlQueueDb = env.openDatabase(null, URL_QUEUE_STORE, dbConfig);
		contentSeenDb = env.openDatabase(null, CONTENT_SEEN_STORE, dbConfig);
		// create bindings
		EntryBinding<String> stringBinding = new StringBinding();
		EntryBinding<Document> documentBinding = new SerialBinding<Document>(javaCatalog, Document.class);
		EntryBinding<UrlQueue> urlQueueBinding = new SerialBinding<UrlQueue>(javaCatalog, UrlQueue.class);
		// create maps
		documentMap = new StoredSortedMap<String, Document>(documentDb, stringBinding, documentBinding, true);
		urlQueueMap = new StoredSortedMap<String, UrlQueue>(urlQueueDb, stringBinding, urlQueueBinding, true);
		contentSeenMap = new StoredSortedMap<String, String>(contentSeenDb, stringBinding, stringBinding, true);
		// initialize urlQueueMap (urlQueueDb) only if necessary
		if (!urlQueueMap.containsKey(URL_QUEUE_MAP_KEY)) {
			urlQueueMap.put(URL_QUEUE_MAP_KEY, new UrlQueue());
		}
	}

	@Override
	public void close() {
		// close databases
		documentDb.close();
		urlQueueDb.close();
		contentSeenDb.close();
		// close the Java class catalog
		javaCatalog.close();
		// close the database environment
		env.close();
	}

	@Override
	public int getCorpusSize() {
		return documentMap.size();
	}

	@Override
	public Set<String> getAllDocumentUrls() {
		return documentMap.keySet();
	}

	@Override
	public void addDocument(String url, String content, String contentType, long contentLength) {
		documentMap.put(url,
				new Document(url, content, contentType, contentLength, ZonedDateTime.now(ZoneId.of("GMT")), false));
	}

	@Override
	public Document getDocument(String url) {
		return documentMap.get(url);
	}

	@Override
	public boolean hasCrawledDocument(String url) {
		return documentMap.containsKey(url);
	}

	@Override
	public String getDocumentContent(String url) {
		return documentMap.containsKey(url) ? documentMap.get(url).getContent() : null;
	}

	@Override
	public String getDocumentContentType(String url) {
		return documentMap.containsKey(url) ? documentMap.get(url).getContentType() : null;
	}

	@Override
	public long getDocumentContentLength(String url) {
		return documentMap.containsKey(url) ? documentMap.get(url).getContentLength() : -1;
	}

	@Override
	public ZonedDateTime getDocumentLastCrawledTime(String url) {
		return documentMap.containsKey(url) ? documentMap.get(url).getLastCrawledTime() : null;
	}

	@Override
	public Set<String> getDocumentOutgoingLinks(String url) {
		return documentMap.containsKey(url) ? documentMap.get(url).getOutgoingLinks() : null;
	}

	@Override
	public void addOutgoingLinkToDocument(String url, String outgoingLink) {
		if (documentMap.containsKey(url)) {
			Document document = documentMap.get(url);
			document.addOutgoingLinks(outgoingLink);
			documentMap.put(url, document);
		}
	}

	@Override
	public void markDocumentAsUploaded(String url) {
		if (documentMap.containsKey(url)) {
			Document document = documentMap.get(url);
			document.setUploaded(true);
			documentMap.put(url, document);
		}
	}

	@Override
	public boolean offerUrlIntoUrlQueue(String url) {
		UrlQueue urlQueue = urlQueueMap.get(URL_QUEUE_MAP_KEY);
		boolean res = urlQueue.offer(url);
		urlQueueMap.put(URL_QUEUE_MAP_KEY, urlQueue);
		return res;
	}

	@Override
	public String pollUrlFromUrlQueue() {
		UrlQueue urlQueue = urlQueueMap.get(URL_QUEUE_MAP_KEY);
		String res = urlQueue.poll();
		urlQueueMap.put(URL_QUEUE_MAP_KEY, urlQueue);
		return res;
	}

	@Override
	public boolean urlQueueIsEmpty() {
		return urlQueueMap.get(URL_QUEUE_MAP_KEY).isEmpty();
	}

	@Override
	public int getUrlQueueSize() {
		return urlQueueMap.get(URL_QUEUE_MAP_KEY).size();
	}

	@Override
	public void addDocumentContentHash(String documentContentHash, String url) {
		contentSeenMap.put(documentContentHash, url);
	}

	@Override
	public boolean hasDocumentContentHash(String documentContentHash) {
		return contentSeenMap.containsKey(documentContentHash);
	}

}
