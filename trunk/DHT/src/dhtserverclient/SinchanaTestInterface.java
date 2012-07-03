/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient;

import dhtserverclient.chord.FingerTableEntry;
import dhtserverclient.thrift.Node;

/**
 *
 * @author Hiru
 */
public interface SinchanaTestInterface {
		
		public abstract void setStable(boolean isStable);
		
		public abstract void setPredecessor(Node predecessor);
		
		public abstract void setSuccessor(Node successor);
		
		public abstract void setRoutingTable(FingerTableEntry[] fingerTableEntrys);	
		
		public abstract void setStatus(String status);
}
