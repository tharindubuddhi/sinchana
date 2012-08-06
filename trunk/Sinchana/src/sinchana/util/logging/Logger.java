/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.logging;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import sinchana.CONFIGURATIONS;

/**
 * 
 * @author Hiru
 */
public final class Logger {

		private static List<Log> logDB = new LinkedList<Log>();
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
//		public static final int CLASS_THRIFT_SERVER = 5;
//		public static final int CLASS_THRIFT_SERVER = 6;
		/**
		 * 
		 */
		public static final int CURRENT_LOG_LEVEL = 2;

		private Logger() {
		}

		/**
		 * 
		 * @param nodeId
		 * @param type
		 * @param classId
		 * @param locId
		 * @param logData
		 */
		public static void log(String nodeId, int type, int classId, int locId, String logData) {
				if (CONFIGURATIONS.DO_LOG) {
						Log nl = new Log();
						nl.nodeId = nodeId;
						nl.level = type;
						nl.classId = classId;
						nl.locId = (byte) locId;
						nl.logData = logData;
						logDB.add(nl);
				}
				if (CURRENT_LOG_LEVEL > type) {
						return;
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
						default:
								className = "LOGGER";
								break;
				}
				java.util.logging.Logger.getLogger(Logger.class.getName()).logp(logLevel,
						className, "Server " + nodeId, logData);


		}

		/**
		 * 
		 */
		public static void print() {
				Iterator<Log> listIterator = logDB.iterator();
				Log log;
				while (listIterator.hasNext()) {
						log = listIterator.next();
						System.out.println(log.toString());
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
				System.out.println("processing quaries...");
				Iterator<Log> listIterator = logDB.iterator();
				Log log;
				boolean filterByNodeId = nodeIds != null && nodeIds.length != 0;
				boolean filterBylevel = levels != null && levels.length != 0;
				boolean filterByClass = classIds != null && classIds.length != 0;
				boolean filterByLocation = locations != null && locations.length != 0;
				boolean filterByText = containTextString.length() > 0;
				boolean validToPrint;
				int recordCount = 0;
				while (listIterator.hasNext()) {
						log = listIterator.next();
						validToPrint = !filterByText || log.logData.indexOf(containTextString) != -1;
						if (!validToPrint) {
								continue;
						}
						if (filterByNodeId) {
								validToPrint = false;
								for (String id : nodeIds) {
										if (log.nodeId.equals(id)) {
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
						System.out.println(log.toString());
				}
				System.out.println(logDB.size() + " records processed. " + recordCount + " matching records found.");
		}
}
