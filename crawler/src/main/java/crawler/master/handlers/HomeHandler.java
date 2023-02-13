package crawler.master.handlers;

import java.util.List;
import java.util.Map;

import crawler.master.MasterServer;
import crawler.master.WorkerInfo;
import spark.Request;
import spark.Response;
import spark.Route;

/**
 * This route handler handles both "/" and "/home" routes on the master side. It
 * displays the home page which allows users to set new crawling tasks and
 * displays crawling information.
 * 
 * @author YuanhongXiao
 *
 */
public class HomeHandler implements Route {

	private List<String> workerKeys = MasterServer.getWorkerKeys();
	private Map<String, WorkerInfo> workerInfos = MasterServer.getWorkerInfos();

	@Override
	public String handle(Request request, Response response) throws Exception {
		response.type("text/html");

		StringBuilder sb = new StringBuilder();

		sb.append("<html><head><title>Distributed Web Crawler</title></head>\n");
		sb.append("<body>\n");
		sb.append("<h1>Distributed Web Crawler Master</h1>");
		sb.append("<hr />\n");

		// section 1: web form
		sb.append("<form method=\"POST\" action=\"/submit-task\">\n");
		sb.append("Start URL: <input type=\"url\" name=\"startUrl\" /><br />\n");
		sb.append(
				"Number of Documents to Crawl: <input type=\"number\" name=\"numOfDocumentsToCrawl\" min=\"1\" step=\"1\" max=\"1000000\" /><br />\n");
		sb.append("<input type=\"submit\" value=\"Start\" />\n");
		sb.append("</form>\n");
		sb.append("<hr />\n");

		// section 2: Stop Crawling button
		sb.append(
				"<form method=\"GET\" action=\"/stop-crawling\"><input type=\"submit\" value=\"Stop Crawling\" /></form>");
		sb.append("<hr />\n");

		// section 3: Current Info
		sb.append("<h3>Number of Documents to Crawl: " + MasterServer.getTotalNumOfDocumentsToCrawl() + "</h3>");
		sb.append("<h3>Number of Crawled Documents: " + MasterServer.getTotalNumOfCrawledDocuments() + "</h3>");
		sb.append("<h3>Number of Uploaded Documents: " + MasterServer.getTotalNumOfUploadedDocuments() + "</h3>");
		sb.append("<hr />\n");

		// section 4: Workers' Info
		sb.append("<h2>Workers' Working Phase: " + MasterServer.getWorkerWorkingPhase() + "</h2>");

		sb.append("<table style=\"width: 100%\">");
		sb.append("<tr>");
		sb.append("<th>Worker #</th>");
		sb.append("<th>IP</th>");
		sb.append("<th>Port</th>");
		sb.append("<th># of Indexed Documents</th>");
		sb.append("<th># of Queuing URLs</th>");
		sb.append("<th># of Uploaded Documents</th>");
		sb.append("</tr>");

		for (int i = 0; i < workerKeys.size(); i++) {
			WorkerInfo workerInfo = workerInfos.get(workerKeys.get(i));
			sb.append("<tr>");
			sb.append("<td align=\"center\">" + i + "</td>");
			sb.append("<td align=\"center\">" + workerInfo.getIp() + "</td>");
			sb.append("<td align=\"center\">" + workerInfo.getPort() + "</td>");
			sb.append("<td align=\"center\">" + workerInfo.getNumOfIndexedDocuments() + "</td>");
			sb.append("<td align=\"center\">" + workerInfo.getNumOfQueuingUrls() + "</td>");
			sb.append("<td align=\"center\">" + workerInfo.getNumOfUploadedDocuments() + "</td>");
			sb.append("</tr>");
		}
		sb.append("</table>");
		sb.append("<hr />\n");

		return sb.append("</body></html>").toString();
	}

}
