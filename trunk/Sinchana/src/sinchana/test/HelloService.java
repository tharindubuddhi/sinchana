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
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void isPublished(byte[] serviceKey, Boolean success) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void isRemoved(byte[] serviceKey, Boolean success) {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
}
