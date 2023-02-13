package crawler.master;

/**
 * This class is used by the crawler master to store the worker information of a
 * single worker sent by that worker.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerInfo {

	private String ip;
	private int port;
	private int numOfIndexedDocuments;
	private int numOfQueuingUrls;
	private int numOfUploadedDocuments;
	private boolean hasStoppedCrawling;

	public WorkerInfo(String ip, int port, int numOfIndexedDocuments, int numOfQueuingUrls, int numOfUploadedDocuments,
			boolean hasStoppedCrawling) {
		this.ip = ip;
		this.port = port;
		this.numOfIndexedDocuments = numOfIndexedDocuments;
		this.numOfQueuingUrls = numOfQueuingUrls;
		this.numOfUploadedDocuments = numOfUploadedDocuments;
		this.hasStoppedCrawling = hasStoppedCrawling;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getNumOfIndexedDocuments() {
		return numOfIndexedDocuments;
	}

	public void setNumOfIndexedDocuments(int numOfIndexedDocuments) {
		this.numOfIndexedDocuments = numOfIndexedDocuments;
	}

	public int getNumOfQueuingUrls() {
		return numOfQueuingUrls;
	}

	public void setNumOfQueuingUrls(int numOfQueuingUrls) {
		this.numOfQueuingUrls = numOfQueuingUrls;
	}

	public int getNumOfUploadedDocuments() {
		return numOfUploadedDocuments;
	}

	public void setNumOfUploadedDocuments(int numOfUploadedDocuments) {
		this.numOfUploadedDocuments = numOfUploadedDocuments;
	}

	public boolean hasStoppedCrawling() {
		return hasStoppedCrawling;
	}

	public void setHasStoppedCrawling(boolean hasStoppedCrawling) {
		this.hasStoppedCrawling = hasStoppedCrawling;
	}

}
