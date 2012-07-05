/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dhtserverclient.chord;

import dhtserverclient.thrift.Node;

/**
 *
 * @author Hiru
 */
public class FingerTableEntry {

		private int start;
		private int end;
		private Node successor;

		public int getEnd() {
				return end;
		}

		public void setEnd(int end) {
				this.end = end;
		}

		public int getStart() {
				return start;
		}

		public void setStart(int start) {
				this.start = start;
		}

		public Node getSuccessor() {
				return successor;
		}

		public void setSuccessor(Node successor) {
				this.successor = successor;
		}

		public boolean isInTheInterval(Node node) {
				if (this.start > this.end) {
						return this.start <= node.serverId || node.serverId >= this.end;
				}
				return this.start <= node.serverId && node.serverId <= this.end;
		}
}
