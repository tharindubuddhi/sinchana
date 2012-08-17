/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.math.BigInteger;
import java.util.Arrays;

/**
 *
 * @author Hiru
 */
public class ByteArrays {

	public static String toReadableString(byte[] arrayToRead) {
		return new BigInteger(1, arrayToRead).toString(16);
	}

	public static byte[] arrayConcat(byte[] array1, byte[] array2) {
		int pos = array1.length;
		byte[] newArray = Arrays.copyOf(array1, pos + array2.length);
		for (byte b : array2) {
			newArray[pos++] = b;
		}
		return newArray;
	}
}
