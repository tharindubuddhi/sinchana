/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.exceptions;

import org.apache.thrift.TException;

/**
 *
 * @author Hiru
 */
public class SinchanaTimeOutException extends TException {

	private static final long serialVersionUID = 1L;

	public SinchanaTimeOutException() {
		super();
	}

	public SinchanaTimeOutException(String message) {
		super(message);
	}

	public SinchanaTimeOutException(Throwable cause) {
		super(cause);
	}

	public SinchanaTimeOutException(String message, Throwable cause) {
		super(message, cause);
	}
}
