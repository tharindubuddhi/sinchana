/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

/**
 *
 * @author Hiru
 */
public class HelloService implements sinchana.service.SinchanaServiceInterface {

	@Override
	public byte[] process(byte[] serviceKey, byte[] data) {
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
	
}
