/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import sinchana.CONFIGURATIONS;
import sinchana.thrift.Node;
import sinchana.util.tools.ByteArrays;

/**
 * 
 * @author Hiru
 */
public final class Logger {

	/**
	 * 
	 */
	public static final int LEVEL_FINE = 0;
	/**
	 * 
	 */
	public static final int LEVEL_INFO = 1;
	/**
	 * 
	 */
	public static final int LEVEL_WARNING = 2;
	/**
	 * 
	 */
	public static final int LEVEL_SEVERE = 3;
	/**
	 * 
	 */
	public static final int CLASS_MESSAGE_HANDLER = 0;
	/**
	 * 
	 */
	public static final int CLASS_ROUTING_TABLE = 1;
	/**
	 * 
	 */
	public static final int CLASS_TESTER = 2;
	/**
	 * 
	 */
	public static final int CLASS_THRIFT_SERVER = 3;
	/**
	 * 
	 */
	public static final int CLASS_CONNECTION_POOL = 4;
	public static final int CLASS_CLIENT_HANDLER = 5;
	public static final int CLASS_SINCHANA_SERVER = 6;
	public static final int CLASS_IO_HANDLER = 7;
	/**
	 * 
	 */
	public static final int CURRENT_LOG_LEVEL = 2;
	private static final int SIZE = 6000;
	private static final Log[] logDB = new Log[SIZE];
	private static int pointer = 0;

	private Logger() {
	}
	private static final Map<String, String> stats = new HashMap<String, String>();

	public static void log(Node node, int type, int classId, int locId, String logData) {
		if (CURRENT_LOG_LEVEL > type) {
			return;
		}
		if (CONFIGURATIONS.DO_LOG) {
			Log nl = new Log();
			nl.node = node;
			nl.level = type;
			nl.classId = classId;
			nl.locId = (byte) locId;
			nl.logData = logData;
			logDB[pointer] = nl;
			pointer = (pointer + 1) % SIZE;
			if (classId == CLASS_IO_HANDLER && locId == 0) {
				stats.put(ByteArrays.idToReadableString(node), logData);
			}

		}

//				System.out.println(nl.toString());
		Level logLevel;
		switch (type) {
			case LEVEL_FINE:
				logLevel = Level.FINE;
				break;
			case LEVEL_INFO:
				logLevel = Level.INFO;
				break;
			case LEVEL_WARNING:
				logLevel = Level.WARNING;
				break;
			case LEVEL_SEVERE:
				logLevel = Level.SEVERE;
				break;
			default:
				logLevel = Level.SEVERE;
				break;
		}
		String className;
		switch (classId) {
			case CLASS_MESSAGE_HANDLER:
				className = "MESSAGE_HANDLER";
				break;
			case CLASS_ROUTING_TABLE:
				className = "ROUTING_TABLE";
				break;
			case CLASS_TESTER:
				className = "TESTER";
				break;
			case CLASS_THRIFT_SERVER:
				className = "THRIFT_SERVER";
				break;
			case CLASS_CONNECTION_POOL:
				className = "CONNECTION_POOL";
				break;
			case CLASS_SINCHANA_SERVER:
				className = "SINCHANA_SERVER";
				break;
			case CLASS_CLIENT_HANDLER:
				className = "CLIENT_HANDLER";
				break;
			case CLASS_IO_HANDLER:
				className = "IO_HANDLER";
				break;
			default:
				className = "LOGGER";
				break;
		}
		java.util.logging.Logger.getLogger(Logger.class.getName()).logp(logLevel,
				className, "Server " + ByteArrays.idToReadableString(node)
				+ " @ " + node.address, logData);


	}

	/**
	 * 
	 */
	public static void print() {
		Set<String> keySet = stats.keySet();
		for (String key : keySet) {
			System.out.println(key + ": " + stats.get(key));
		}
	}

	/**
	 * 
	 * @param nodeIds
	 * @param levels
	 * @param classIds
	 * @param locations
	 */
	public static synchronized void print(String[] nodeIds, int[] levels,
			int[] classIds, int[] locations, String containTextString) {
		System.out.println("processing quaries... " + SIZE);
		int x = pointer, s = logDB.length;
		boolean filterByNodeId = nodeIds != null && nodeIds.length != 0;
		boolean filterBylevel = levels != null && levels.length != 0;
		boolean filterByClass = classIds != null && classIds.length != 0;
		boolean filterByLocation = locations != null && locations.length != 0;
		boolean filterByText = containTextString.length() > 0;
		boolean validToPrint;
		int recordCount = 0;
		Log log;
		for (x = pointer; x < SIZE; x++) {
			if (logDB[x] == null) {
				continue;
			}
			log = logDB[x];
			x++;

			validToPrint = !filterByText || log.logData.indexOf(containTextString) != -1;
			if (!validToPrint) {
				continue;
			}
			if (filterByNodeId) {
				validToPrint = false;
				for (String id : nodeIds) {
					if (id.equals(ByteArrays.idToReadableString(log.node))) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			if (filterBylevel) {
				validToPrint = false;
				for (int i : levels) {
					if (log.level == i) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			if (filterByClass) {
				validToPrint = false;
				for (int i : classIds) {
					if (log.classId == i) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			if (filterByLocation) {
				validToPrint = false;
				for (int i : locations) {
					if (log.locId == i) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			recordCount++;
//			System.out.println(log.toString());
		}
		for (x = 0; x < pointer; x++) {
			if (logDB[x] == null) {
				continue;
			}
			log = logDB[x];
			x++;

			validToPrint = !filterByText || log.logData.indexOf(containTextString) != -1;
			if (!validToPrint) {
				continue;
			}
			if (filterByNodeId) {
				validToPrint = false;
				for (String id : nodeIds) {
					if (id.equals(ByteArrays.idToReadableString(log.node))) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			if (filterBylevel) {
				validToPrint = false;
				for (int i : levels) {
					if (log.level == i) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			if (filterByClass) {
				validToPrint = false;
				for (int i : classIds) {
					if (log.classId == i) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			if (filterByLocation) {
				validToPrint = false;
				for (int i : locations) {
					if (log.locId == i) {
						validToPrint = true;
						break;
					}
				}
			}
			if (!validToPrint) {
				continue;
			}
			recordCount++;
//			System.out.println(log.toString());
		}

		System.out.println(SIZE + " records processed. " + recordCount + " matching records found.");
		Set<String> keySet = stats.keySet();
		for (String key : keySet) {
			System.out.println(key + ": " + stats.get(key));
		}
	}
}
