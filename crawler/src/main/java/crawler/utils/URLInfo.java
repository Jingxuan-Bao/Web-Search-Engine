package crawler.utils;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * This class is used by both the crawler master and crawler workers to parse
 * URLs and try to make sure that the URL is not malformed and valid.
 * 
 * @author YuanhongXiao
 *
 */
public class URLInfo {

	private URL url;
	private boolean isSecure;
	private String authority;
	private String hostName;
	private int portNumber;
	private String path;

	public URLInfo(String urlString) throws MalformedURLException, URISyntaxException {
		url = new URL(urlString);
		url.toURI(); // for checking validity only

		isSecure = url.getProtocol().equals("https");
		authority = url.getAuthority();
		hostName = url.getHost();
		portNumber = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
		path = url.getPath();
	}

	public boolean isSecure() {
		return isSecure;
	}

	public String getAuthority() {
		return authority;
	}

	public String getHostName() {
		return hostName;
	}

	public int getPortNumber() {
		return portNumber;
	}

	public String getPath() {
		return path;
	}

	@Override
	public String toString() {
		return url.toString();
	}

}
