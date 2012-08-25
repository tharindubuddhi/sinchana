/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author Hiru
 */
public class Hash {

	private static final String ALGORITHM = "SHA-1";
	private static final String ENCODING = "utf8";
	private static final String ERROR = "Error calculating hash value";

	public static byte[] generateId(String address) {
		try {
			MessageDigest cript = MessageDigest.getInstance(ALGORITHM);
			cript.reset();
			cript.update(address.getBytes(ENCODING));
			byte[] digest = cript.digest();
			if (digest.length != 20) {
				throw new RuntimeException();
			}
			return digest;
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ERROR, ex);
		} catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException(ERROR, ex);
		}
	}
}
