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
public class SinchanaInterruptedException extends TException {

	private static final long serialVersionUID = 1L;

	public SinchanaInterruptedException() {
		super();
	}

	public SinchanaInterruptedException(String message) {
		super(message);
	}

	public SinchanaInterruptedException(Throwable cause) {
		super(cause);
	}

	public SinchanaInterruptedException(String message, Throwable cause) {
		super(message, cause);
	}
}
