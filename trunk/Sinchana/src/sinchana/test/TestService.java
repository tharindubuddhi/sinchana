/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana.test;

import sinchana.Server;
import sinchana.SinchanaServiceInterface;

/**
 *
 * @author DELL
 */
public class TestService {

	private Server server;
	private int testId;
	private ServerUI gui = null;
	private TesterController testerController;

	public TestService(Tester tester, TesterController tc) {

		this.testId = tester.getTestId();
		this.testerController = tc;
		this.server = tester.getServer();
		this.gui = tester.getGui();


		server.registerSinchanaServiceInterface(new SinchanaServiceInterface() {

			@Override
			public boolean publish(String key, String service) {
				System.out.println(service + " published with the key " + key);
				return true;
			}

			@Override
			public String get(String key) {
				throw new UnsupportedOperationException("Not supported yet.");
			}

			@Override
			public boolean remove(String key) {
				throw new UnsupportedOperationException("Not supported yet.");
			}
		});

	}
}
