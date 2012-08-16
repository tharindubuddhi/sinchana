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
public class CommonTools {

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

	public static String toReadableString(byte[] arrayToRead) {
		return new BigInteger(1, arrayToRead).toString(16);
	}

	public static byte[] arrayConcat(byte[] array1, byte[] array2) {
		int length = array1.length + array2.length;
		int pos = 0;
		byte[] newArray = new byte[length];
		for (byte b : array1) {
			newArray[pos++] = b;
		}
		for (byte b : array2) {
			newArray[pos++] = b;
		}
		return newArray;
	}
}
