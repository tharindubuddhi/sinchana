/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.logging;

import sinchana.thrift.Node;


/**
 *
 * @author Hiru
 */
public class Log {

	Node node;
	int level;
	int classId;
	int locId;
	String logData;

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(node).append(":");
		switch (level) {
			case Logger.LEVEL_FINE:
				sb.append("\tFINE");
				break;
			case Logger.LEVEL_INFO:
				sb.append("\tINFO");
				break;
			case Logger.LEVEL_WARNING:
				sb.append("\tWARNING");
				break;
			case Logger.LEVEL_SEVERE:
				sb.append("\tSEVERE");
				break;

		}
		switch (classId) {
			case Logger.CLASS_MESSAGE_HANDLER:
				sb.append("\tMESSAGE_HANDLER");
				break;
			case Logger.CLASS_ROUTING_TABLE:
				sb.append("\tROUTING_TABLE");
				break;
			case Logger.CLASS_TESTER:
				sb.append("\tTESTER");
				break;
			case Logger.CLASS_THRIFT_SERVER:
				sb.append("\tTHRIFT_SERVER");
				break;
			case Logger.CLASS_CONNECTION_POOL:
				sb.append("\tCONNECTION_POOL");
				break;
		}
		sb.append("\t").append(locId);
		sb.append("\n\t").append(logData);
		return sb.toString();
	}
}
