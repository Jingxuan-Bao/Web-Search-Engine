package crawler.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import crawler.worker.WorkerParameters;

/**
 * This class is used by the crawler worker to get the robots.txt information so
 * that the crawler can try to be a good citizen. In this project, we assume
 * that the "User-agent" is always "cis455crawler". Also, only "Disallow:" and
 * "Crawl-delay:" are respected. Wildcard characters are not supported.
 * 
 * @author YuanhongXiao
 *
 */
public class RobotsTxtInfo {

	private static final String CIS_455_CRAWLER = WorkerParameters.getUserAgent();

	// Map<User-agent, List<path>>
	private Map<String, List<String>> disallows;
	// Map<User-agent, crawl delay (in seconds)>
	private Map<String, Integer> crawlDelays;

	/**
	 * Use this constructor only if failed to get robots.txt for the host
	 */
	public RobotsTxtInfo() {
		disallows = new HashMap<String, List<String>>();
		disallows.put("*", new ArrayList<String>());
		disallows.put(CIS_455_CRAWLER, new ArrayList<String>());
		crawlDelays = new HashMap<String, Integer>();
		crawlDelays.put("*", 0);
		crawlDelays.put(CIS_455_CRAWLER, 0);
	}

	public RobotsTxtInfo(URLInfo urlInfo) throws NumberFormatException, IOException {
		disallows = new HashMap<String, List<String>>();
		disallows.put("*", new ArrayList<String>());
		disallows.put(CIS_455_CRAWLER, new ArrayList<String>());
		crawlDelays = new HashMap<String, Integer>();
		crawlDelays.put("*", 0);
		crawlDelays.put(CIS_455_CRAWLER, 0);

		String authority = urlInfo.getAuthority();
		BufferedReader br;
		String robotsTxtUrlString;

		if (urlInfo.isSecure()) { // https
			robotsTxtUrlString = "https://" + authority + "/robots.txt";
			URL robotsTxtUrl = new URL(robotsTxtUrlString);
			HttpsURLConnection conn = (HttpsURLConnection) robotsTxtUrl.openConnection();
			conn.setRequestProperty("User-Agent", CIS_455_CRAWLER);
			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		} else { // http
			robotsTxtUrlString = "http://" + authority + "/robots.txt";
			URL robotsTxtUrl = new URL(robotsTxtUrlString);
			HttpURLConnection conn = (HttpURLConnection) robotsTxtUrl.openConnection();
			conn.setRequestProperty("User-Agent", CIS_455_CRAWLER);
			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		}

		String line = null;
		String userAgent = "";

		while ((line = br.readLine()) != null) {
			if (line.startsWith("User-agent:")) {
				userAgent = line.replace("User-agent:", "").trim();
			} else if (userAgent.equals("*") || userAgent.equals(CIS_455_CRAWLER)) {
				// only care about User-agent "*" and "cis455crawler"
				// don't need to implement "Allow:", see piazza question @844
				if (line.startsWith("Disallow:")) { // Disallow:
					disallows.get(userAgent).add(line.replace("Disallow:", "").trim());
				} else if (line.startsWith("Crawl-delay:")) { // Crawl-delay:
					crawlDelays.put(userAgent, Integer.parseInt(line.replace("Crawl-delay:", "").trim()));
				}
			}
		}
	}

	public int getCrawlDelay() {
		return crawlDelays.get(CIS_455_CRAWLER) > 0 ? crawlDelays.get(CIS_455_CRAWLER) : crawlDelays.get("*");
	}

	/**
	 * https://developers.google.com/search/docs/advanced/robots/robots_txt#url-matching-based-on-path-values
	 * 
	 * @param path
	 * @return
	 */
	public boolean disallowPath(String path) {
		return !disallows.get(CIS_455_CRAWLER).isEmpty() ? disallowPathHelper(path, disallows.get(CIS_455_CRAWLER))
				: disallowPathHelper(path, disallows.get("*"));
	}

	private boolean disallowPathHelper(String path, List<String> disallowPaths) {
		for (String disallowPath : disallowPaths) {
			if (disallowPath.equals("/")) {
				return true;
			}
			if (disallowPath.endsWith("/") && path.contains(disallowPath)) {
				return true;
			}
			if (!disallowPath.endsWith("/") && path.startsWith(disallowPath)) {
				return true;
			}
		}
		return false;
	}

}
