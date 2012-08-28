/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test.examples;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Hiru
 */
public class HelloService implements sinchana.service.SinchanaServiceInterface {
	
	@Override
	public byte[] process(byte[] serviceKey, byte[] data) {
//		try {
//			Thread.sleep(10000);
//		} catch (InterruptedException ex) {
//			Logger.getLogger(HelloService.class.getName()).log(Level.SEVERE, null, ex);
//		}
		return ("Hi " + new String(data) + ", Greetings from " + new String(serviceKey)).getBytes();
	}
	
	@Override
	public void isPublished(byte[] serviceKey, Boolean success) {
		System.out.println(new String(serviceKey) + " is published");
	}
	
	@Override
	public void isRemoved(byte[] serviceKey, Boolean success) {
		System.out.println(new String(serviceKey) + " is removed");
	}
	
	@Override
	public void error(byte[] error) {
		throw new UnsupportedOperationException("Not supported yet.");
	}
}
