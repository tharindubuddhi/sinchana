/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.messagequeue;

import java.util.concurrent.Semaphore;
import sinchana.thrift.Message;

/**
 * This class implements a message queue. The messages are stored in a circular 
 * buffer (array) and served in FIFS manner by a thread dedicated to the queue.  
 * @author Hiru
 */
public class MessageQueue implements Runnable {

		private int MESSAGE_BUFFER_SIZE;
		private int head = 0;
		private int tail = 0;
		private Message[] messageQueue;
		private Semaphore messagesAvailable = new Semaphore(0);
		private MessageEventHandler messageEventHandler;
		private Thread[] threads = null;
		private boolean started;
		private int numberOfThreads = 1;

		/**
		 * Initialize a message queue.
		 * @param size	Size of the message queue.
		 * @param meh	MessageEventHandler instance which is used to callback 
		 * when messages are received.
		 */
		public MessageQueue(int size, MessageEventHandler meh, int numOfThreads) {
				this.messageEventHandler = meh;
				this.numberOfThreads = numOfThreads;
				this.MESSAGE_BUFFER_SIZE = size;
				this.messageQueue = new Message[MESSAGE_BUFFER_SIZE];
				this.threads = new Thread[numOfThreads];
				for (int i = 0; i < threads.length; i++) {
						threads[i] = new Thread(new Runnable() {

								@Override
								public void run() {
										Message message;
										while (true) {
												try {
														messagesAvailable.acquire();
														synchronized (messageQueue) {
																message = messageQueue[tail];
																tail = (tail + 1) % MESSAGE_BUFFER_SIZE;
														}
														messageEventHandler.process(message);
												} catch (InterruptedException ex) {
														throw new RuntimeException(ex);
												}
										}
								}
						});
				}
				started = false;
		}

		/**
		 * Returns the message queue thread id.
		 * @return id of the message queue thread.
		 */
		public long[] getThreadIds() {
				long[] ids = new long[numberOfThreads];
				for (int i = 0; i < ids.length; i++) {
						ids[i] = threads[i].getId();
				}
				return ids;
		}

		/**
		 * Start serving the message queue. A new thread will start and serve when messages are received.
		 * @return		Id of the thread which is dedicated to the queue.
		 */
		public synchronized void start() {
				started = true;
				for (int i = 0; i < threads.length; i++) {
						if (!threads[i].isAlive()) {
								threads[i].start();
						}
				}
		}

		/**
		 * Reset the message queue.
		 */
		public void reset() {
				messagesAvailable.drainPermits();
				head = 0;
				tail = 0;
		}

		/**
		 * Queue a message to the message queue. This method is synchronized.
		 * @param message		Message to add in to the queue
		 * @return				True if message is added to the queue. False if the queue is full.
		 */
		public synchronized boolean queueMessage(Message message) {
				if ((tail + MESSAGE_BUFFER_SIZE - head) % MESSAGE_BUFFER_SIZE == 1) {
						return false;
				}
				messageQueue[head] = message;
				head = (head + 1) % MESSAGE_BUFFER_SIZE;
				messagesAvailable.release();
				return true;
		}

		/**
		 * Returns whether the message queue is empty or not.
		 * @return		True if the queue is empty. Otherwise, false.
		 */
		public boolean isEmpty() {
				return tail == head;
		}

		/**
		 * Returns whether the message queue has started or not;
		 * @return true if the queue is serving, false otherwise.
		 */
		public boolean isStarted() {
				return started;
		}

		/**
		 * Returns the size of the message queue. This returns the number of un-served message in the queue.
		 * @return		Number of unserved messages in the queue.
		 */
		public int size() {
				return (head + MESSAGE_BUFFER_SIZE - tail) % MESSAGE_BUFFER_SIZE;
		}

		@Override
		public void run() {
		}

		public interface MessageEventHandler {

				/**
				 * This method will be called when the message queue serves messages.
				 * @param message		The message currently serving.
				 */
				public abstract void process(Message message);
		}
}
