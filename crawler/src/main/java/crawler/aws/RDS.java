package crawler.aws;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import crawler.worker.WorkerParameters;
import crawler.worker.WorkerStatus;
import crawler.worker.storage.Document;
import crawler.worker.storage.IWorkerStorage;
import crawler.worker.storage.WorkerStorageSingleton;

/**
 * This class contains static functions to do operations with AWS RDS (connect,
 * upload, close).
 * 
 * @author YuanhongXiao
 *
 */
public class RDS {

	private static final Logger logger = LogManager.getLogger(RDS.class);

	private static Connection conn;
	private static Statement stmt;

//	private static final String CONNECTION_URL = "jdbc:mysql://cis555db.cpe2gquyawpv.us-east-1.rds.amazonaws.com:3306/cis555?user=admin&password=cis45555";
	private static final String CONNECTION_URL = "jdbc:mysql://" + WorkerParameters.getRdsDbName()
			+ ".cpe2gquyawpv.us-east-1.rds.amazonaws.com:3306/cis555?user=" + WorkerParameters.getRdsDbUsername()
			+ "&password=" + WorkerParameters.getRdsDbPassword();
	private static final String DOCUMENTS = WorkerParameters.getRdsDbDocumentsTableName();
	private static final String LINKS = WorkerParameters.getRdsDbLinksTableName();

	private static IWorkerStorage db = WorkerStorageSingleton
			.getWorkerStorageInstance(WorkerParameters.getStorageDirectory());

	public static void connect() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.cj.jdbc.Driver");
		conn = DriverManager.getConnection(CONNECTION_URL);
		stmt = conn.createStatement();
	}

	// https://stackoverflow.com/questions/2225221/closing-database-connections-in-java
	public static void close() throws SQLException {
		if (stmt != null) {
			stmt.close();
		}
		if (conn != null) {
			conn.close();
		}
	}

	public static void upload() {
		Set<String> urls = db.getAllDocumentUrls();

		for (String url : urls) {
			Document document = db.getDocument(url);
			if (document.isUploaded()) { // document is already uploaded
				WorkerStatus.incrementNumOfUploadedDocuments();
				logger.info(
						"Worker on port " + WorkerParameters.getPort() + " has already uploaded the document " + url);
			} else {
				try {
					documentsInsertQuery(url, document.getContentType(), document.getContent());
					for (String outgoingLink : document.getOutgoingLinks()) {
						linksInsertQuery(url, outgoingLink);
					}
					db.markDocumentAsUploaded(url);
					WorkerStatus.incrementNumOfUploadedDocuments();
					logger.info("Worker on port " + WorkerParameters.getPort()
							+ " just successfully uploaded the document " + url);
				} catch (SQLException e) {
					logger.error(
							"Worker on port " + WorkerParameters.getPort() + " failed to upload the document " + url,
							e);
				}
			}
		}
	}

	// https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
	private static void documentsInsertQuery(String url, String type, String content) throws SQLException {
		url = url.replace("'", "''");
		type = type.replace("'", "''");
		content = content.replace("'", "''");

		StringBuilder query = new StringBuilder("INSERT INTO " + DOCUMENTS + "\n");
		query.append("VALUES ('" + url + "', '" + type + "', '" + content + "')\n");
		query.append("ON DUPLICATE KEY UPDATE type = '" + type + "', content = '" + content + "';");

		stmt.executeUpdate(query.toString());
	}

	private static void linksInsertQuery(String srcUrl, String destUrl) throws SQLException {
		srcUrl = srcUrl.replace("'", "''");
		destUrl = destUrl.replace("'", "''");

		StringBuilder query = new StringBuilder("INSERT INTO " + LINKS + "\n");
		query.append("VALUES ('" + srcUrl + "', '" + destUrl + "')\n");
		query.append("ON DUPLICATE KEY UPDATE src_url = '" + srcUrl + "', dest_url = '" + destUrl + "';");

		stmt.executeUpdate(query.toString());
	}

}
