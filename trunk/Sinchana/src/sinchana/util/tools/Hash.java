/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public class Hash {

	public static byte[] generateId(String address) {
		try {
			MessageDigest cript = MessageDigest.getInstance("SHA-1");
			cript.reset();
			cript.update(address.getBytes("utf8"));
			byte[] digest = cript.digest();
			if (digest.length != 20) {
				throw new RuntimeException();
			}
			return digest;
		} catch (Exception ex) {
			throw new RuntimeException("Error calculating hash value", ex);
		}
	}
}
