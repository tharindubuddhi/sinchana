/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.pastry;

import java.math.BigInteger;
import java.util.Set;
import sinchana.RoutingHandler;
import sinchana.Server;
import sinchana.thrift.Message;
import sinchana.thrift.Node;

/**
 *
 * @author DELL
 */
public class PastryTable implements RoutingHandler{

    public static final int IDSPACE = 160;
    public static final int BASE = 16;
    public static final int TABLE_SIZE = (int) (Math.log(Math.pow(2, IDSPACE))/Math.log(BASE));
	private static final int SUCCESSOR_LEVEL = 3;
	private final Server server;
	private final Node[] successor = new Node[SUCCESSOR_LEVEL];
	private final Node[] predecessor = new Node[SUCCESSOR_LEVEL];
	private String serverId;
	private BigInteger serverIdAsBigInt;
	private final Node[][] fingerTable = new Node[TABLE_SIZE][10];
	private boolean neighboursImported = false;

    public PastryTable(Server server) {
        this.server = server;
    }
    
    
    @Override
    public void init() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Node[] getSuccessors() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Node[] getPredecessors() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Node getNextNode(String destination) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Node> getNeighbourSet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Node> getFailedNodeSet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void optimize() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void getOptimalSuccessor(Message message) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeNode(Node node) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeNode(Set<Node> nodes) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTable(Node node, boolean ignorePrevFailures) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public static void main(String[] args) {
        System.out.println(TABLE_SIZE);
    }
}
