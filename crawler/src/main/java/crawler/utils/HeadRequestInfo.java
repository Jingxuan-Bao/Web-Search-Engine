package crawler.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import javax.net.ssl.HttpsURLConnection;

import crawler.worker.WorkerParameters;
import crawler.worker.storage.IWorkerStorage;

/**
 * This class is used by the crawler worker to make a HEAD request to a URL to
 * get some header information.
 * 
 * @author YuanhongXiao
 *
 */
public class HeadRequestInfo {

	private static final String CIS_455_CRAWLER = WorkerParameters.getUserAgent();

	private String contentType;
	private long contentLength;
	private boolean isModified;
	private ZonedDateTime lastCrawledTime;

	public HeadRequestInfo(URLInfo urlInfo, IWorkerStorage db) throws IOException {
		// see conn.getLastModified()
		ZonedDateTime baseTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT"));
		String urlString = urlInfo.toString();
		URL url = new URL(urlString);

		if (db.hasCrawledDocument(urlString)) {
			lastCrawledTime = db.getDocumentLastCrawledTime(urlString);
			contentType = db.getDocumentContentType(urlString);
			contentLength = db.getDocumentContentLength(urlString);
		} else {
			lastCrawledTime = ZonedDateTime.of(1900, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT"));
		}

		if (urlInfo.isSecure()) { // https
			HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
			conn.setRequestMethod("HEAD");
			conn.setRequestProperty("User-Agent", CIS_455_CRAWLER);
			conn.setRequestProperty("If-Modified-Since", lastCrawledTime.format(DateTimeFormatter.RFC_1123_DATE_TIME));
			if (isModified = baseTime.plusNanos(conn.getLastModified() * 1000000).isAfter(lastCrawledTime)) {
				contentType = parseContentType(conn.getContentType());
				contentLength = conn.getContentLengthLong();
			}
		} else { // http
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("HEAD");
			conn.setRequestProperty("User-Agent", CIS_455_CRAWLER);
			conn.setRequestProperty("If-Modified-Since", lastCrawledTime.format(DateTimeFormatter.RFC_1123_DATE_TIME));
			if (isModified = baseTime.plusNanos(conn.getLastModified() * 1000000).isAfter(lastCrawledTime)) {
				contentType = parseContentType(conn.getContentType());
				contentLength = conn.getContentLengthLong();
			}
		}
	}

	private String parseContentType(String rawContentType) {
		if (rawContentType == null) {
			return "null";
		}
		if (rawContentType.contains(";")) {
			return rawContentType.split(";")[0].trim();
		}
		return rawContentType.trim();
	}

	public String getContentType() {
		return contentType;
	}

	public long getContentLength() {
		return contentLength;
	}

	public boolean isModified() {
		return isModified;
	}

}
