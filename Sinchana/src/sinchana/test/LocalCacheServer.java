/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import sinchana.CONFIGURATIONS;

/**
 *
 * @author Hiru
 */
public class LocalCacheServer {

		private static final int CACHE_SIZE = 20;
		private static final String[] NODES = new String[CACHE_SIZE];

		private static String getRemoteNode(String address) {
				boolean contains = false;
				for (int i = 0; i < CACHE_SIZE; i++) {
						if (NODES[i] != null && NODES[i].equals(address)) {
								contains = true;
						}
				}
				if (!contains) {
						for (int i = 0; i < CACHE_SIZE; i++) {
								if (NODES[i] == null) {
										NODES[i] = address;
										break;
								}
						}
				}


				String reply = null;
				int val, count = 0;
				while (reply == null && count < 1000000) {
						val = (int) (Math.random() * CACHE_SIZE);
						if (NODES[val] != null && !NODES[val].equals(address)) {
								reply = NODES[val];
						}
						count++;
				}

				return reply;
		}

		public static void clear() {
				if (CONFIGURATIONS.USE_REMOTE_CACHE_SERVER) {
						try {
								URL yahoo = new URL("http://cseanremo.appspot.com/remoteip?clear=true");
								URLConnection yc = yahoo.openConnection();
								InputStreamReader isr = new InputStreamReader(yc.getInputStream());
								isr.close();
						} catch (Exception e) {
								throw new RuntimeException("Error in clearing the cache server.", e);
						}
				} else {
						for (int i = 0; i < CACHE_SIZE; i++) {
								NODES[i] = null;
						}
				}
		}

		public static String getRemoteNode(String address, int portId) {
				if (CONFIGURATIONS.USE_REMOTE_CACHE_SERVER) {
						try {
								URL url = new URL("http://cseanremo.appspot.com/remoteip?"
										+ "sid=" + address + "@" + portId
										+ "&url=" + address
										+ "&pid=" + portId);
								URLConnection yc = url.openConnection();
								InputStreamReader isr = new InputStreamReader(yc.getInputStream());
								BufferedReader in = new BufferedReader(isr);
								String resp = in.readLine();
								in.close();
								System.out.println("resp: " + resp);
								if (resp == null || resp.equalsIgnoreCase("null")) {
										throw new RuntimeException("Error in getting remote node");
								}
								if (!resp.equalsIgnoreCase("n/a")) {
										return resp.split(":")[1] + ":" + resp.split(":")[2];
								}
						} catch (Exception e) {
								throw new RuntimeException("Invalid response from the cache server!", e);
						}
						return null;
				} else {
						return getRemoteNode(address + ":" + portId);
				}
		}
}
