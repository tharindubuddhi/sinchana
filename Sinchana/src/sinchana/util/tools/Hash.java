/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public class Hash {

		public static long generateId(byte[] address, short portId, long gridSize) {
				byte msb = 1;
				byte lsb = 1;
				try {
						MessageDigest cript = MessageDigest.getInstance("SHA-1");
						cript.reset();
						cript.update((new String(address) + portId).getBytes("utf8"));
						byte[] digest = cript.digest();
						lsb = digest[digest.length - 1];
//						lsb = digest[0];
				} catch (UnsupportedEncodingException | NoSuchAlgorithmException ex) {
						throw new RuntimeException("Error calculating hash value", ex);
				}

				long id = 0;
				int t0 = portId % 256;
				int t1 = portId / 256;

				id += t0;
				id *= 256;

				id += ((address[3] + 256) % 256);
				id *= 256;

				id += ((address[1] + 256) % 256);
				id *= 256;

				id += t1;
				id *= 256;

				id += ((address[2] + 256) % 256);
				id *= 256;

				id += ((address[0] + 256) % 256);
				
				id = id % gridSize;

				check(id);
				return id;
		}

		public static long generateId(String address, short portId, long gridSize) {
				try {
						MessageDigest cript = MessageDigest.getInstance("SHA-1");
						cript.reset();
						cript.update(address.getBytes("utf8"));
						byte[] digest = cript.digest();
						long tid = 1;
						int temp;
						for (int i = digest.length - 3; i < digest.length; i++) {
								temp = ((digest[i] + 256) % 256);
								tid *= temp;
						}
						tid *= portId;
						long id = tid % gridSize;
						check(id);
						return id;
				} catch (UnsupportedEncodingException | NoSuchAlgorithmException ex) {
						throw new RuntimeException("Error calculating hash value", ex);
				}
		}
		private static Set<Long> ids = new HashSet<>();

		private static synchronized void check(long id) {
				if (ids.contains(id)) {
						throw new RuntimeException("Duplicating ID " + id + "! Go & find a new hash function :P");
				}
				ids.add(id);
		}
}
