/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.logging;

import sinchana.thrift.Node;

/**
 *
 * @author Hiru & Hatim.
 */
public class Log {

	Node node;
	int locId;
	String logData;
        Logger.LogLevel logLevel;
        Logger.LogClass classId;

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(node).append(":");
                //Logger.LogLevel level = Logger.LogLevel.LEVEL_FINE;
                //Logger.LogClass classId = Logger.LogClass.CLASS_MESSAGE_HANDLER;
		switch (logLevel) {
			case LEVEL_FINE:
				sb.append("\tFINE");
				break;
			case LEVEL_INFO:
				sb.append("\tINFO");
				break;
			case LEVEL_WARNING:
				sb.append("\tWARNING");
				break;
			case LEVEL_SEVERE:
				sb.append("\tSEVERE");
				break;

		}
		switch (classId) {
			case CLASS_MESSAGE_HANDLER:
				sb.append("\tMESSAGE_HANDLER");
				break;
			case CLASS_ROUTING_TABLE:
				sb.append("\tROUTING_TABLE");
				break;
			case CLASS_TESTER:
				sb.append("\tTESTER");
				break;
			case CLASS_THRIFT_SERVER:
				sb.append("\tTHRIFT_SERVER");
				break;
			case CLASS_CONNECTION_POOL:
				sb.append("\tCONNECTION_POOL");
				break;
			case CLASS_IO_HANDLER:
				sb.append("\tIO_HANDLER");
				break;
			case CLASS_CLIENT_HANDLER:
				sb.append("\tCLIENT_HANDLER");
				break;
			default:
				sb.append("\tLOGGER");
				break;
		}
		sb.append("\t").append(locId);
		sb.append("\n\t").append(logData);
		return sb.toString();
	}
}
