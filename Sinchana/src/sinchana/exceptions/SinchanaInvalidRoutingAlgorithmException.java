/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.exceptions;


/**
 *
 * @author Hiru
 */
public class SinchanaInvalidRoutingAlgorithmException extends Exception {

	private static final long serialVersionUID = 1L;

	public SinchanaInvalidRoutingAlgorithmException() {
		super();
	}

	public SinchanaInvalidRoutingAlgorithmException(String message) {
		super(message);
	}

	public SinchanaInvalidRoutingAlgorithmException(Throwable cause) {
		super(cause);
	}

	public SinchanaInvalidRoutingAlgorithmException(String message, Throwable cause) {
		super(message, cause);
	}
}
