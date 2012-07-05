/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient;

import dhtserverclient.thrift.Node;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public interface RoutingHandler {
		
		public static final int TABLE_SIZE = 10;
		public static final int GRID_SIZE = (int) Math.pow(2, TABLE_SIZE);
		
		public abstract void init();
		
		public abstract Node getSuccessor();
		
		public abstract Node getPredecessor();
		
		public abstract Node getNextNode(int destination);
		
		public abstract Set<Node> getNeighbourSet();
		
		public abstract void setNeighbourSet(Set<Node> neighbourSet);
		
		public abstract Node getOptimalSuccessor(int rServerId, int startOfRange);
		
		public abstract void setOptimalSuccessor(int startOfRange, Node successor);
		
		public abstract void updateTable(Node node);
		
		public abstract void setStable(boolean isStable);
		
		public abstract boolean isStable();
		
}
