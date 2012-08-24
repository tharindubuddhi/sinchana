/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

/**
 *
 * @author Hiru
 */
public interface SinchanaTestInterface {

	/**
	 * 
	 * @param isStable
	 */
	public abstract void setStable(boolean isStable);

	public abstract void setMessageQueueSize(int size);

	public abstract void incRequestCount(int lifetime, boolean routedViaPredecessors);
	
	public abstract void incIncomingMessageCount();
}
