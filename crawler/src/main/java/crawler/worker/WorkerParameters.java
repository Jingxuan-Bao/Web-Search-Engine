package crawler.worker;

/**
 * This class stores a crawler worker's parameters which are manually provided
 * by people who run the worker.
 * 
 * @author YuanhongXiao
 *
 */
public class WorkerParameters {

	private static final String USER_AGENT = "cis455crawler";

	// Parameters for both crawling and uploading
	private static String masterAddress;
	private static int port;
	private static String storageDirectory;

	// Parameters for crawling only
	private static int maxDocumentSize;
	private static int maxUrlLength;

	// Parameters for uploading to AWS only
	private static String rdsDbUsername;
	private static String rdsDbPassword;
	private static String rdsDbName;
	private static String rdsDbDocumentsTableName;
	private static String rdsDbLinksTableName;

	public static String getUserAgent() {
		return USER_AGENT;
	}

	public static String getMasterAddress() {
		return masterAddress;
	}

	public static void setMasterAddress(String masterAddress) {
		WorkerParameters.masterAddress = masterAddress;
	}

	public static int getPort() {
		return port;
	}

	public static void setPort(int port) {
		WorkerParameters.port = port;
	}

	public static String getStorageDirectory() {
		return storageDirectory;
	}

	public static void setStorageDirectory(String storageDirectory) {
		WorkerParameters.storageDirectory = storageDirectory;
	}

	public static int getMaxDocumentSize() {
		return maxDocumentSize;
	}

	public static void setMaxDocumentSize(int maxDocumentSize) {
		WorkerParameters.maxDocumentSize = maxDocumentSize;
	}

	public static int getMaxUrlLength() {
		return maxUrlLength;
	}

	public static void setMaxUrlLength(int maxUrlLength) {
		WorkerParameters.maxUrlLength = maxUrlLength;
	}

	public static String getRdsDbUsername() {
		return rdsDbUsername;
	}

	public static void setRdsDbUsername(String rdsDbUsername) {
		WorkerParameters.rdsDbUsername = rdsDbUsername;
	}

	public static String getRdsDbPassword() {
		return rdsDbPassword;
	}

	public static void setRdsDbPassword(String rdsDbPassword) {
		WorkerParameters.rdsDbPassword = rdsDbPassword;
	}

	public static String getRdsDbName() {
		return rdsDbName;
	}

	public static void setRdsDbName(String rdsDbName) {
		WorkerParameters.rdsDbName = rdsDbName;
	}

	public static String getRdsDbDocumentsTableName() {
		return rdsDbDocumentsTableName;
	}

	public static void setRdsDbDocumentsTableName(String rdsDbDocumentsTableName) {
		WorkerParameters.rdsDbDocumentsTableName = rdsDbDocumentsTableName;
	}

	public static String getRdsDbLinksTableName() {
		return rdsDbLinksTableName;
	}

	public static void setRdsDbLinksTableName(String rdsDbLinksTableName) {
		WorkerParameters.rdsDbLinksTableName = rdsDbLinksTableName;
	}

}