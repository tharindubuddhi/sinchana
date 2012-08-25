/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.dataStore;

import sinchana.SinchanaCallBack;

/**
 *
 * @author Hiru
 */
public interface SinchanaDataCallback extends SinchanaCallBack {

	public abstract void isStored(byte[] key, boolean success);

	public abstract void isRemoved(byte[] key, boolean success);

	public abstract void response(byte[] key, byte[] data);
}
