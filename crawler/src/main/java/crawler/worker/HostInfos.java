package crawler.worker;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import crawler.utils.RobotsTxtInfo;

/**
 * This class stores the robots.txt information and last crawled time for every
 * host name. These information are in memory, not on disk.
 * 
 * @author YuanhongXiao
 *
 */
public class HostInfos {

	// ConcurrentMap<host name, robots.txt info>
	private static ConcurrentMap<String, RobotsTxtInfo> robotsTxtInfosMap = new ConcurrentHashMap<String, RobotsTxtInfo>();
	// ConcurrentMap<host name, last crawled time>
	private static ConcurrentMap<String, ZonedDateTime> lastCrawledTimesMap = new ConcurrentHashMap<String, ZonedDateTime>();

	public static boolean hasRobotsTxtInfo(String hostName) {
		return robotsTxtInfosMap.containsKey(hostName);
	}

	public static RobotsTxtInfo getRobotsTxtInfo(String hostName) {
		return robotsTxtInfosMap.get(hostName);
	}

	public static void setRobotsTxtInfo(String hostName, RobotsTxtInfo robotsTxtInfo) {
		robotsTxtInfosMap.put(hostName, robotsTxtInfo);
	}

	public static ZonedDateTime getLastCrawledTime(String hostName) {
		return lastCrawledTimesMap.get(hostName);
	}

	public static void updateLastCrawledTime(String hostName) {
		lastCrawledTimesMap.put(hostName, ZonedDateTime.now(ZoneId.of("GMT")));
	}

}