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

	public abstract Node[] getSuccessors();

	public abstract Node[] getPredecessors();

	/**
	 * Returns the next hop for the destination according to the finger table.
	 * @param destination Destination id.
	 * @return NExt hop (node) to reach to the destination.
	 */
	public abstract Node getNextNode(byte[] destination);

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

	public abstract boolean updateTable(Node node, boolean add);
	
	public abstract void printInfo();
}
