/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.routing.chord;

import java.math.BigInteger;
import sinchana.thrift.Node;

/**
 *
 * @author Hiru
 */
public class FingerTableEntry {

	private BigInteger start;
	private BigInteger startOffset;
	private BigInteger end;
	private BigInteger endOffset;
	private Node[] successors;
	
	public FingerTableEntry(int numOfTableEntries){
		this.successors = new Node[numOfTableEntries];
	}

	public BigInteger getEnd() {
		return end;
	}

	public void setEnd(BigInteger end) {
		this.end = end;
	}

	public BigInteger getStart() {
		return start;
	}

	public void setStart(BigInteger start) {
		this.start = start;
	}

	public BigInteger getEndOffset() {
		return endOffset;
	}

	public void setEndOffset(BigInteger endOffset) {
		this.endOffset = endOffset;
	}

	public BigInteger getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(BigInteger startOffset) {
		this.startOffset = startOffset;
	}
	
	public Node[] getSuccessors() {
		return successors;
	}

	public void setSuccessor(Node successor, int index) {
		this.successors[index] = successor;
	}

	public boolean isInTheInterval(Node node) {
		BigInteger bi = new BigInteger(1, node.serverId.array());
		if (this.start.compareTo(this.end) == 1) {
			return this.start.compareTo(bi) != 1 || bi.compareTo(this.end) != -1;
		}
		return this.start.compareTo(bi) != 1 && bi.compareTo(this.end) != 1;
	}
}
