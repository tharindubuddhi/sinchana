/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;


/**
 *
 * @author Hiru
 */
public class LocalCacheServer {

		private static final int CACHE_SIZE = 20;
		private static final String[] NODES = new String[CACHE_SIZE];

		public static String getRemoteNode(String address) {
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
				for (int i = 0; i < CACHE_SIZE; i++) {
						NODES[i] = null;
				}
		}
}
