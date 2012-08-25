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
public interface SinchanaServiceInterface extends SinchanaCallBack {

	public abstract byte[] process(byte[] serviceKey, byte[] data);

	public abstract void isPublished(byte[] serviceKey, Boolean success);

	public abstract void isRemoved(byte[] serviceKey, Boolean success);
    
}
