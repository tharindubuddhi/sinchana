/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.util.messagequeue;

import java.util.concurrent.Semaphore;
import sinchana.thrift.Message;

/**
 *
 * @author Hiru
 */
public class MessageQueue implements Runnable {

		private int MESSAGE_BUFFER_SIZE;
		private int head = 0;
		private int tail = 0;
		private Message[] messageQueue;
		private Semaphore messagesAvailable = new Semaphore(0);
		private MessageEventHandler messageEventHandler;
		private Thread thread;

		public MessageQueue(int sizeOfQueue, MessageEventHandler meh) {
				this.messageEventHandler = meh;
				MESSAGE_BUFFER_SIZE = sizeOfQueue;
				this.messageQueue = new Message[MESSAGE_BUFFER_SIZE];
		}

		public long start() {
				if (thread == null) {
						thread = new Thread(this);
						thread.start();
				}
				return thread.getId();
		}

		public synchronized boolean queueMessage(Message message) {
				if ((tail + MESSAGE_BUFFER_SIZE - head) % MESSAGE_BUFFER_SIZE == 1) {
						return false;
				}
				messageQueue[head] = message;
				head = (head + 1) % MESSAGE_BUFFER_SIZE;
				messagesAvailable.release();
				return true;
		}

		public boolean isEmpty() {
				return tail == head;
		}

		@Override
		public void run() {
				while (true) {
						try {
								messagesAvailable.acquire();
								Message message = messageQueue[tail];
								tail = (tail + 1) % MESSAGE_BUFFER_SIZE;
								this.messageEventHandler.process(message);
						} catch (InterruptedException ex) {
								ex.printStackTrace();
						}
				}
		}
}
