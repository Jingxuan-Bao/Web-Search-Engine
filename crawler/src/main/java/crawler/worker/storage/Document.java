package crawler.worker.storage;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;

/**
 * This class stores all information of a crawled URL document. It is used in
 * the Berkeley DB for permanent storage (on disk).
 * 
 * @author YuanhongXiao
 *
 */
public class Document implements Serializable {

	private static final long serialVersionUID = 1L;

	private String url; // key of a Document object
	private String content;
	private String contentType;
	private long contentLength;
	private ZonedDateTime lastCrawledTime;
	private boolean isUploaded;
	private Set<String> outgoingLinks;

	public Document(String url, String content, String contentType, long contentLength, ZonedDateTime lastCrawledTime,
			boolean isUploaded) {
		this.url = url;
		this.content = content;
		this.contentType = contentType;
		this.contentLength = contentLength;
		this.lastCrawledTime = lastCrawledTime;
		this.isUploaded = isUploaded;
		this.outgoingLinks = new HashSet<String>();
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public long getContentLength() {
		return contentLength;
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	public ZonedDateTime getLastCrawledTime() {
		return lastCrawledTime;
	}

	public void setLastCrawledTime(ZonedDateTime lastCrawledTime) {
		this.lastCrawledTime = lastCrawledTime;
	}

	public boolean isUploaded() {
		return isUploaded;
	}

	public void setUploaded(boolean isUploaded) {
		this.isUploaded = isUploaded;
	}

	public Set<String> getOutgoingLinks() {
		return outgoingLinks;
	}

	public void setOutgoingLinks(Set<String> outgoingLinks) {
		this.outgoingLinks = outgoingLinks;
	}

	public void addOutgoingLinks(String outgoingLink) {
		outgoingLinks.add(outgoingLink);
	}

}
