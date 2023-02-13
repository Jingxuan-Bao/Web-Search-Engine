package crawler.worker.storage;

import java.time.ZonedDateTime;
import java.util.Set;

/**
 * This class is the interface for permanent data storage on disk by using
 * Berkeley DB. The storage on disk keeps data of all crawled URL documents, the
 * URL queue and all seen document contents to save time.
 * 
 * @author YuanhongXiao
 *
 */
public interface IWorkerStorage {

	/**
	 * Shuts down / flushes / closes the storage system
	 */
	public void close();

	/**
	 * Get the number of crawled documents
	 * 
	 * @return
	 */
	public int getCorpusSize();

	/**
	 * Get all documents' URLs (keys)
	 * 
	 * @return
	 */
	public Set<String> getAllDocumentUrls();

	/**
	 * Add a new document (URL)
	 * 
	 * @param url
	 * @param content
	 * @param contentType
	 * @param contentLength
	 */
	public void addDocument(String url, String content, String contentType, long contentLength);

	/**
	 * Retrieve a Document object by URL
	 * 
	 * @param url
	 * @return
	 */
	public Document getDocument(String url);

	/**
	 * Check whether has crawled a URL
	 * 
	 * @param url
	 * @return
	 */
	public boolean hasCrawledDocument(String url);

	/**
	 * Retrieve a document's content by URL
	 * 
	 * @param url
	 * @return
	 */
	public String getDocumentContent(String url);

	/**
	 * Retrieve a document's content type by URL
	 * 
	 * @param url
	 * @return
	 */
	public String getDocumentContentType(String url);

	/**
	 * Retrieve a document's content length by URL
	 * 
	 * @param url
	 * @return
	 */
	public long getDocumentContentLength(String url);

	/**
	 * Retrieve a document's last crawled time by URL
	 * 
	 * @param url
	 * @return
	 */
	public ZonedDateTime getDocumentLastCrawledTime(String url);

	/**
	 * Retrieve a document's outgoing links by URL
	 * 
	 * @param url
	 * @return
	 */
	public Set<String> getDocumentOutgoingLinks(String url);

	/**
	 * Add an outgoing link to a document by URL
	 * 
	 * @param url
	 * @param outgoingLink
	 */
	public void addOutgoingLinkToDocument(String url, String outgoingLink);

	/**
	 * Mark a document as uploaded by URL
	 * 
	 * @param url
	 */
	public void markDocumentAsUploaded(String url);

	/**
	 * 
	 * @param url
	 * @return true if the element was added to this queue, else false
	 */
	public boolean offerUrlIntoUrlQueue(String url);

	/**
	 * 
	 * @return the head of this queue, or null if this queue is empty
	 */
	public String pollUrlFromUrlQueue();

	/**
	 * 
	 * @return true if the URL queue is empty
	 */
	public boolean urlQueueIsEmpty();

	/**
	 * 
	 * @return the size of the URL queue
	 */
	public int getUrlQueueSize();

	/**
	 * Add the MD5 hash of the document content
	 * 
	 * @param documentContentHash
	 * @param url
	 */
	public void addDocumentContentHash(String documentContentHash, String url);

	/**
	 * Check if the document content has been seen by its MD5 hash
	 * 
	 * @param documentContentHash
	 * @return
	 */
	public boolean hasDocumentContentHash(String documentContentHash);

}
