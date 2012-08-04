/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class LocalCacheServer {

		private static final int CACHE_SIZE = 20;
		private static final Node[] NODES = new Node[CACHE_SIZE];

		public static Node getRemoteNode(Node node) {
				boolean contains = false;
				for (int i = 0; i < CACHE_SIZE; i++) {
						if (NODES[i] != null && NODES[i].serverId.equals(node.serverId)) {
								contains = true;
						}
				}
				if (!contains) {
						for (int i = 0; i < CACHE_SIZE; i++) {
								if (NODES[i] == null) {
										NODES[i] = node.deepCopy();
										break;
								}
						}
				}


				Node reply = null;
				int val, count = 0;
				while (reply == null && count < 1000000) {
						val = (int) (Math.random() * CACHE_SIZE);
						if (NODES[val] != null && !NODES[val].serverId.equals(node.serverId)) {
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
