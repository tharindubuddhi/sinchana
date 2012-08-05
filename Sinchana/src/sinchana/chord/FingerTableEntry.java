/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.chord;

import java.math.BigInteger;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class FingerTableEntry {

		private BigInteger start;
		private BigInteger end;
		private Node successor;

		/**
		 * 
		 * @return
		 */
		public BigInteger getEnd() {
				return end;
		}

		/**
		 * 
		 * @param end
		 */
		public void setEnd(BigInteger end) {
				this.end = end;
		}

		/**
		 * 
		 * @return
		 */
		public BigInteger getStart() {
				return start;
		}

		/**
		 * 
		 * @param start
		 */
		public void setStart(BigInteger start) {
				this.start = start;
		}

		/**
		 * 
		 * @return
		 */
		public Node getSuccessor() {
				return successor;
		}

		/**
		 * 
		 * @param successor
		 */
		public void setSuccessor(Node successor) {
				this.successor = successor;
		}

		/**
		 * 
		 * @param node
		 * @return
		 */
		public boolean isInTheInterval(Node node) {
				BigInteger bi = new BigInteger(node.serverId, 16);
				if (this.start.compareTo(this.end) == 1) {
						return this.start.compareTo(bi) != 1 || bi.compareTo(this.end) != -1;
				}
				return this.start.compareTo(bi) != 1 && bi.compareTo(this.end) != 1;
		}
}
