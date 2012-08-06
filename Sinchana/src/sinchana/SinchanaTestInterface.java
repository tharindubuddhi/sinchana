/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.chord.FingerTableEntry;
import sinchana.thrift.Node;

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

	/**
	 * 
	 * @param predecessor
	 */
	public abstract void setPredecessor(Node predecessor);

	/**
	 * 
	 * @param successor
	 */
	public abstract void setSuccessor(Node successor);

	/**
	 * 
	 * @param fingerTableEntrys
	 */
	public abstract void setRoutingTable(FingerTableEntry[] fingerTableEntrys);

	/**
	 * 
	 * @param status
	 */
	public abstract void setStatus(String status);

	/**
	 * 
	 * @param isRunning
	 */
	public abstract void setServerIsRunning(boolean isRunning);

	public abstract void incIncomingMessageCount();

	public abstract void setMessageQueueSize(int size);

	public abstract void setOutMessageQueueSize(int size);
}
