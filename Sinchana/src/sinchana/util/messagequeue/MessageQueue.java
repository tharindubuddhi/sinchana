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
public class MessageQueue {

	public static final int PRIORITY_NONE = 0;
	public static final int PRIORITY_LOW = 1;
	public static final int PRIORITY_HIGH = 2;
	private final int MESSAGE_BUFFER_SIZE;
	private int head = 0;
	private int tail = 0;
	private final Message[] messageQueue;
	private final Semaphore messagesAvailable = new Semaphore(0);
	private final Semaphore queueLockLowPriority = new Semaphore(0);
	private final Semaphore queueLockHighPriority = new Semaphore(0);
	private boolean waitOnLowPriorityQueue = false;
	private boolean waitOnHighPriorityQueue = false;
	private final MessageEventHandler messageEventHandler;
	private final Thread[] threads;
	private boolean started;
	private final int numberOfThreads;
	private Message firstMessage = null;

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
					synchronized (messageQueue) {
						if (firstMessage != null) {
							messageEventHandler.process(firstMessage);
							firstMessage = null;
						}
					}
					Message message;
					while (true) {
						try {
							messagesAvailable.acquire();
							synchronized (messageQueue) {
								message = messageQueue[tail];
								tail = (tail + 1) % MESSAGE_BUFFER_SIZE;
								if (waitOnHighPriorityQueue) {
									waitOnHighPriorityQueue = false;
									queueLockHighPriority.release();
								} else if (waitOnLowPriorityQueue) {
									waitOnLowPriorityQueue = false;
									queueLockLowPriority.release();
								}
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

	public void validateThread() {
		long tid = Thread.currentThread().getId();
		for (int i = 0; i < threads.length; i++) {
			if (tid == threads[i].getId()) {
				return;
			}
		}
		throw new RuntimeException("Invalid thead access!");
	}

	/**
	 * Start serving the message queue. A new thread will start and serve when messages are received.
	 * @return		Id of the thread which is dedicated to the queue.
	 */
	public synchronized void start() {
		if (started) {
			return;
		}
		started = true;
		for (int i = 0; i < threads.length; i++) {
			if (!threads[i].isAlive()) {
				threads[i].start();
			}
		}
	}

	public synchronized void start(Message message) {
		if (started) {
			return;
		}
		firstMessage = message;
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
	private synchronized boolean queueMessage(Message message) {
		if ((tail + MESSAGE_BUFFER_SIZE - head) % MESSAGE_BUFFER_SIZE == 1) {
			return false;
		}
		messageQueue[head] = message;
		head = (head + 1) % MESSAGE_BUFFER_SIZE;
		messagesAvailable.release();
		return true;
	}

	public boolean queueMessageAndWait(Message message, int priority) {
		switch (priority) {
			case PRIORITY_HIGH:
				while (!queueMessage(message)) {
					waitOnHighPriorityQueue = true;
					queueLockHighPriority.acquireUninterruptibly();
				}
				return true;
			case PRIORITY_LOW:
				while (!queueMessage(message)) {
					waitOnLowPriorityQueue = true;
					queueLockLowPriority.acquireUninterruptibly();
				}
				return true;
			case PRIORITY_NONE:
			default:
				return !waitOnHighPriorityQueue && !waitOnLowPriorityQueue && queueMessage(message);
		}
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

	public interface MessageEventHandler {

		/**
		 * This method will be called when the message queue serves messages.
		 * @param message		The message currently serving.
		 */
		public abstract void process(Message message);
	}
}
