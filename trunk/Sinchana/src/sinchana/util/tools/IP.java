/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 * @author Hiru
 */
public class IP {

		public static void main(String[] args) {
				getLocalHost();
		}

		public static void getLocalHost() {
				try {
						InetAddress address = InetAddress.getLocalHost();
						System.out.println(address.getHostAddress());
						System.out.println(address.getHostName());
						byte[] address1 = address.getAddress();
						for (byte b : address1) {
								System.out.print((b + 256) % 256 + " ");
						}
						System.out.println();
				} catch (UnknownHostException ex) {
						ex.printStackTrace();
				}
		}
}
