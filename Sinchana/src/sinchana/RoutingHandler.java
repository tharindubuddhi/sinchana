/************************************************************************************

 * Sinchana Distributed Hash table 

 * Copyright (C) 2012 Sinchana DHT - Department of Computer Science &               
 * Engineering, University of Moratuwa, Sri Lanka. Permission is hereby 
 * granted, free of charge, to any person obtaining a copy of this 
 * software and associated documentation files of Sinchana DHT, to deal 
 * in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.

 * Neither the name of University of Moratuwa, Department of Computer Science 
 * & Engineering nor the names of its contributors may be used to endorse or 
 * promote products derived from this software without specific prior written 
 * permission.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
 * SOFTWARE.                                                                    
 ************************************************************************************/
package sinchana;

import java.util.Set;
import sinchana.thrift.Node;

/**
 *This interface provide the common interface for the routing tables. 
 * If new routing table is to added, it should implement this interface.
 * @author Hiru
 */
public interface RoutingHandler {

	/**
	 * Initialize routing table.
	 */
	public abstract void init();

	/**
	 * Returns the successors as a Node array
	 * @return successors
	 */
	public abstract Node[] getSuccessors();

	/**
	 * Returns the predecessors as a Node array
	 * @return predecessors
	 */
	public abstract Node[] getPredecessors();

	/**
	 * Returns the next hop for the destination according to the finger table.
	 * @param destination byte array destination which should be exactly 20 bytes. 
	 * @return Next hop (node) to reach to the destination.
	 */
	public abstract Node getNextNode(byte[] destination);

	/**
	 * Returns the set of nodes contains successor, predecessor and all the
	 * table entries.
	 * @return Set of neighbor nodes.
	 */
	public abstract Set<Node> getNeighbourSet();

	/**
	 * Checks whether the node is in at least one of the routing table, successors or in predecessors.
	 * @param nodeToCkeck node to be checked
	 * @return <code>true</code> if the node is in the routing table, successors or in predecessors. 
	 * <code>false</code> otherwise.
	 */
	public abstract boolean isInTheTable(Node nodeToCkeck);

	/**
	 * Triggers the routing table optimization functions.
	 */
	public abstract void triggerOptimize();

	/**
	 * Update the table with the node. Returns whether any of the routing table, successors or predecessors 
	 * were updated by the node.
	 * @param node node to be updated.
	 * @param add <code>true</code> if the node is joining. <code>false</code> if leaving.
	 * @return <code>true</code> if the table is changed by the node, <code>false</code> otherwise.
	 */
	public abstract boolean updateTable(Node node, boolean add);

	/**
	 * Print routing table, successors and predecessors in the console.
	 */
	public abstract void printInfo();
}
