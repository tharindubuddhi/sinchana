/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException("Error calculating hash value", ex);
		} catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException("Error calculating hash value", ex);
		}
	}
}
