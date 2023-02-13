package crawler.worker.storage;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class serves as the URL queue. It can be kept permanently on disk by
 * using Berkeley DB.
 * 
 * @author YuanhongXiao
 *
 */
public class UrlQueue implements Serializable {

	private static final long serialVersionUID = 1L;

	private BlockingQueue<String> urlQueue;
	private Set<String> urlSet;

	public UrlQueue() {
		urlQueue = new LinkedBlockingQueue<String>();
		urlSet = new HashSet<String>();
	}

	/**
	 * 
	 * @param url
	 * @return true if the element was added to this queue, else false
	 */
	public boolean offer(String url) {
		if (urlSet.contains(url)) {
			return false;
		}

		if (urlQueue.offer(url)) {
			urlSet.add(url);
			return true;
		}

		return false;
	}

	/**
	 * 
	 * @return the head of this queue, or null if this queue is empty
	 */
	public String poll() {
		String url = urlQueue.poll();

		if (url != null) {
			urlSet.remove(url);
		}

		return url;
	}

	public boolean isEmpty() {
		return urlQueue.isEmpty();
	}

	/**
	 * For testing purpose only
	 * 
	 * @return
	 */
	public int size() {
		return urlQueue.size();
	}

}
