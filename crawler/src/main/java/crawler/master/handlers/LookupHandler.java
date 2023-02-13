package crawler.master.handlers;

import static spark.Spark.halt;

import javax.servlet.http.HttpServletResponse;

import crawler.utils.URLInfo;
import crawler.worker.storage.IWorkerStorage;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles the "/lookup" route on the master side. It serves
 * to testing purpose only.
 * 
 * @author YuanhongXiao
 *
 */
public class LookupHandler implements Route {

	private IWorkerStorage db;

	public LookupHandler(IWorkerStorage db) {
		this.db = db;
	}

	@Override
	public String handle(Request request, Response response) throws Exception {
		String queryUrl = request.queryParams("url");
		String url = new URLInfo(queryUrl).toString();

		if (db.hasCrawledDocument(url)) { // the URL is crawled
			return db.getDocumentContent(url);
		}

		// the URL is not crawled
		halt(HttpServletResponse.SC_NOT_FOUND, "url=" + queryUrl + " is Not Found");

		return "";
	}

}
