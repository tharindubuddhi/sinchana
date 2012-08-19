/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;


/**
 *
 * @author Hiru
 */
public interface SinchanaResponseHandler extends SinchanaCallBackHandler{

	public abstract void response(byte[] message);
}
