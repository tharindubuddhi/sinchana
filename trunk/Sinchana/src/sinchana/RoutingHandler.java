/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Set;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public interface RoutingHandler {

	/**
	 * Initialize routing table.
	 */
	public abstract void init();

	/**
	 * Returns the successor.
	 * @return successor node.
	 */
	public abstract Node[] getSuccessors();

	/**
	 * Returns the predecessor.
	 * @return predecessor node.
	 */
	public abstract Node[] getPredecessors();

	/**
	 * Returns the next hop for the destination according to the finger table.
	 * @param destination Destination id.
	 * @return NExt hop (node) to reach to the destination.
	 */
	public abstract Node getNextNode(byte[] destination, byte[] lastHop);

	/**
	 * Returns the set of nodes contains successor, predecessor and all the
	 * table entries.
	 * @return Set of neighbor nodes.
	 */
	public abstract Set<Node> getNeighbourSet();

	/**
	 * Triggers the routing table optimization functions.
	 */
	public abstract void triggerOptimize();

	/**
	 * Returns the most optimal node from the neighbor set which is successor 
	 * to the start of range.
	 * @param message  Request message object.
	 * @return Node which immediately follows start point.
	 */
//	public abstract void getOptimalSuccessor(Message message);
	/**
	 * Removes the node from the predecessor, successor and routing table entries. 
	 * Blanked locations will be temporary filled with the matching nodes from 
	 * the neighbor set.
	 * @param node Node to remove from the routing table.
	 */
	/**
	 * Updates the table, successor and predecessor with the new node.
	 * @param node Node to be added to the routing table.
	 */
	public abstract boolean updateTable(Node node, boolean add);
	
	public abstract void printInfo();
}
