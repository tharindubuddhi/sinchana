/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class ByteArrays {

	public static String idToReadableString(Node node) {
		return idToReadableString(node.serverId.array());
	}
	
	public static String idToReadableString(ByteBuffer byteBuffer) {
		return idToReadableString(byteBuffer.array());
	}

	public static String idToReadableString(byte[] arrayToRead) {
		String val = new BigInteger(1, arrayToRead).toString(16).toUpperCase();
		return (val.length() == 40 ? val : "0" + val);
	}

	public static String toReadableString(ByteBuffer byteBuffer) {
		return toReadableString(byteBuffer.array());
	}

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
