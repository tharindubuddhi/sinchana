/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import sinchana.thrift.Node;
import java.util.Set;

/**
 *
 * @author Hiru
 */
public interface RoutingHandler {
		
		/**
		 * Size of the finger table in Chord.
		 */
		public static final int TABLE_SIZE = 10;
		/**
		 * Size of the network which is equal to 
		 * <code>(int) Math.pow(2, RoutingHandler.TABLE_SIZE)</code>
		 */
		public static final int GRID_SIZE = (int) Math.pow(2, RoutingHandler.TABLE_SIZE);
		
		/**
		 * Initialize routing table.
		 */
		public abstract void init();
		
		/**
		 * Returns the successor.
		 * @return successor node.
		 */
		public abstract Node getSuccessor();
		
		/**
		 * Returns the predecessor.
		 * @return predecessor node.
		 */
		public abstract Node getPredecessor();
		
		/**
		 * Returns the next hop for the destination according to the finger table.
		 * @param destination Destination id.
		 * @return NExt hop (node) to reach to the destination.
		 */
		public abstract Node getNextNode(int destination);
		
		/**
		 * Returns the set of nodes contains successor, predecessor and all the
		 * table entries.
		 * @return Set of neighbor nodes.
		 */
		public abstract Set<Node> getNeighbourSet();
		
		/**
		 * Triggers the routing table optimization functions.
		 */
		public abstract void optimize();
		
		/**
		 * Returns the most optimal node from the neighbor set which is successor 
		 * to the start of range.
		 * @param id Start point.
		 * @return Node which immediately follows start point.
		 */
		public abstract Node getOptimalSuccessor(int id);
		
		/**
		 * Removes the node from the predecessor, successor and routing table entries. 
		 * Blanked locations will be temporary filled with the matching nodes from 
		 * the neighbor set.
		 * @param node Node to remove from the routing table.
		 */
		public abstract void removeNode(Node node);
		
		/**
		 * Updates the table, successor and predecessor with the new node.
		 * @param node Node to be added to the routing table.
		 */
		public abstract void updateTable(Node node);
		
		/**
		 * 
		 * @param isStable
		 */
		public abstract void setStable(boolean isStable);
		
		/**
		 * 
		 * @return
		 */
		public abstract boolean isStable();
		
}
