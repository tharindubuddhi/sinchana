/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public class CommonTools {

		public static BigInteger generateId(String address) {
				try {
						MessageDigest cript = MessageDigest.getInstance("SHA-1");
						cript.reset();
						cript.update(address.getBytes("utf8"));
						byte[] digest = cript.digest();
						BigInteger bi = new BigInteger(1, digest);
						check(bi);
						return bi;
				} catch (Exception ex) {
						throw new RuntimeException("Error calculating hash value", ex);
				}
		}
		private static Set<BigInteger> ids = new HashSet<BigInteger>();

		private static synchronized void check(BigInteger id) {
				if (ids.contains(id) || id.toString(16).length() > 40) {
						throw new RuntimeException("Wrong ID " + id + "! Go & find a new hash function :P");
				}
				ids.add(id);
		}
}
