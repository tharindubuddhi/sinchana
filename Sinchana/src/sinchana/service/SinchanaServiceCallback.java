/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.service;

import sinchana.SinchanaCallBack;

/**
 *
 * @author Hiru
 */
public interface SinchanaServiceCallback extends SinchanaCallBack {

	public abstract void serviceFound(byte[] key, boolean success, byte[] data);

	public abstract void serviceResponse(byte[] key, boolean success, byte[] data);
}
