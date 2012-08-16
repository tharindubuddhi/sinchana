/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;


/**
 *
 * @author Hiru
 */
public interface SinchanaRequestHandler extends SinchanaCallBackHandler{

	public abstract byte[] request(byte[] message);
	
}
