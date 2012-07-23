/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.chord;

import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class FingerTableEntry {

		private long start;
		private long end;
		private Node successor;

		/**
		 * 
		 * @return
		 */
		public long getEnd() {
				return end;
		}

		/**
		 * 
		 * @param end
		 */
		public void setEnd(long end) {
				this.end = end;
		}

		/**
		 * 
		 * @return
		 */
		public long getStart() {
				return start;
		}

		/**
		 * 
		 * @param start
		 */
		public void setStart(long start) {
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
				if (this.start > this.end) {
						return this.start <= node.serverId || node.serverId >= this.end;
				}
				return this.start <= node.serverId && node.serverId <= this.end;
		}
}
