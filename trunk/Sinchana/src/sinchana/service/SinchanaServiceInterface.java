/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.service;

import sinchana.SinchanaCallBackHandler;

/**
 *
 * @author Hiru
 */
public interface SinchanaServiceInterface extends SinchanaCallBackHandler {

	public abstract byte[] process(byte[] serviceKey, byte[] data);

	/**
	 * 
	 * @param success boolean value whether the dataObject published successfully
	 */
    
	public abstract void isPublished(byte[] serviceKey, Boolean success);

	/**
	 * 
	 * @param success boolean value whether the dataObject removed successfully
	 */
	public abstract void isRemoved(byte[] serviceKey, Boolean success);
    
}
