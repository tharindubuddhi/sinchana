/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * ControllerUI.java
 *
 * Created on Jan 19, 2012, 10:46:11 PM
 */
package sinchana.test;

import javax.swing.table.DefaultTableModel;

/**
 *
 * @author Hiru
 */
public class ControllerUI extends javax.swing.JFrame implements Runnable {

		DefaultTableModel dtm;
		Object[] nodes;
		Object[] expectedCount;
		Object[] recievedCount;
		Object[] duration;
		private TesterController testerController;
		private long startNodeThreadId = 0;
		private long startAutoTestThreadId = 0;

		/** Creates new form ControllerUI
		 * @param tc 
		 */
		public ControllerUI(TesterController tc) {
				this.testerController = tc;
				initComponents();
				setLocationRelativeTo(null);
				this.numOfTesters.setText("" + TesterController.NUM_OF_TESTING_NODES);
				this.numOfAutoTesters.setText("" + TesterController.NUM_OF_AUTO_TESTING_NODES);

		}

		/**
		 * 
		 * @param coloms
		 */
		public void initTableInfo(String[] coloms) {
				dtm = new DefaultTableModel();
				dtm.setColumnIdentifiers(coloms);
				dtm.setColumnCount(coloms.length);
				table.setModel(dtm);
		}

		/**
		 * 
		 * @param data
		 */
		public void setTableInfo(Object[][] data) {
				dtm.setRowCount(data.length);
				for (int i = 0; i < data.length; i++) {
						int length = data[i].length;
						for (int j = 0; j < length; j++) {
								dtm.setValueAt(data[i][j], i, j);
						}
				}
		}

		/**
		 * 
		 * @param status
		 */
		public void setStatus(String status) {
				this.statusField.setText(status);
		}

		public void setStat(String status) {
				this.statField.setText(status);
		}

		/** This method is called from within the constructor to
		 * initialize the form.
		 * WARNING: Do NOT modify this code. The content of this method is
		 * always regenerated by the Form Editor.
		 */
		@SuppressWarnings("unchecked")
        // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
        private void initComponents() {
                java.awt.GridBagConstraints gridBagConstraints;

                jPanel1 = new javax.swing.JPanel();
                autoTestButtonPanel = new javax.swing.JPanel();
                startNodeSetButton = new javax.swing.JButton();
                portRange = new javax.swing.JTextField();
                numOfTesters = new javax.swing.JTextField();
                ringTestButton = new javax.swing.JButton();
                numOfAutoTesters = new javax.swing.JTextField();
                autoTestButton = new javax.swing.JButton();
                sendMessagePanel = new javax.swing.JPanel();
                jLabel5 = new javax.swing.JLabel();
                reqId = new javax.swing.JTextField();
                jLabel6 = new javax.swing.JLabel();
                destId = new javax.swing.JTextField();
                jLabel7 = new javax.swing.JLabel();
                message = new javax.swing.JTextField();
                sendButton = new javax.swing.JButton();
                statPanel = new javax.swing.JPanel();
                statField = new javax.swing.JLabel();
                servicePanel = new javax.swing.JPanel();
                egLabel = new javax.swing.JLabel();
                egButton = new javax.swing.JButton();
                egTextField = new javax.swing.JTextField();
                jScrollPane1 = new javax.swing.JScrollPane();
                table = new javax.swing.JTable();
                statusField = new javax.swing.JLabel();
                loggerControlPanel = new javax.swing.JPanel();
                jLabel1 = new javax.swing.JLabel();
                logNodeID = new javax.swing.JTextField();
                jLabel2 = new javax.swing.JLabel();
                logType = new javax.swing.JTextField();
                jLabel3 = new javax.swing.JLabel();
                logClass = new javax.swing.JTextField();
                jLabel4 = new javax.swing.JLabel();
                logLocation = new javax.swing.JTextField();
                jLabel9 = new javax.swing.JLabel();
                contatain = new javax.swing.JTextField();
                printLogButton = new javax.swing.JButton();

                setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
                getContentPane().setLayout(new java.awt.GridBagLayout());

                jPanel1.setLayout(new java.awt.GridLayout(4, 1, 0, 4));

                autoTestButtonPanel.setBorder(javax.swing.BorderFactory.createBevelBorder(javax.swing.border.BevelBorder.RAISED));

                startNodeSetButton.setText("Start Node Set");
                startNodeSetButton.setPreferredSize(new java.awt.Dimension(134, 24));
                startNodeSetButton.addActionListener(new java.awt.event.ActionListener() {
                        public void actionPerformed(java.awt.event.ActionEvent evt) {
                                startNodeSetButtonActionPerformed(evt);
                        }
                });
                autoTestButtonPanel.add(startNodeSetButton);

                portRange.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                portRange.setText("8000");
                portRange.setToolTipText("No of Testing Nodes");
                portRange.setPreferredSize(new java.awt.Dimension(32, 24));
                autoTestButtonPanel.add(portRange);

                numOfTesters.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                numOfTesters.setText("12");
                numOfTesters.setToolTipText("No of Testing Nodes");
                numOfTesters.setPreferredSize(new java.awt.Dimension(32, 24));
                autoTestButtonPanel.add(numOfTesters);

                ringTestButton.setText("Test Ring");
                ringTestButton.setEnabled(false);
                ringTestButton.setPreferredSize(new java.awt.Dimension(134, 24));
                ringTestButton.addActionListener(new java.awt.event.ActionListener() {
                        public void actionPerformed(java.awt.event.ActionEvent evt) {
                                ringTestButtonActionPerformed(evt);
                        }
                });
                autoTestButtonPanel.add(ringTestButton);

                numOfAutoTesters.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                numOfAutoTesters.setText("12");
                numOfAutoTesters.setToolTipText("No of Auto Testing Nodes");
                numOfAutoTesters.setPreferredSize(new java.awt.Dimension(32, 24));
                autoTestButtonPanel.add(numOfAutoTesters);

                autoTestButton.setText("Test Message System");
                autoTestButton.setEnabled(false);
                autoTestButton.setPreferredSize(new java.awt.Dimension(134, 24));
                autoTestButton.addActionListener(new java.awt.event.ActionListener() {
                        public void actionPerformed(java.awt.event.ActionEvent evt) {
                                autoTestButtonActionPerformed(evt);
                        }
                });
                autoTestButtonPanel.add(autoTestButton);

                jPanel1.add(autoTestButtonPanel);

                sendMessagePanel.setBorder(javax.swing.BorderFactory.createBevelBorder(javax.swing.border.BevelBorder.RAISED));

                jLabel5.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel5.setText("Sender");
                jLabel5.setPreferredSize(new java.awt.Dimension(56, 24));
                sendMessagePanel.add(jLabel5);

                reqId.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                reqId.setText("0");
                reqId.setPreferredSize(new java.awt.Dimension(56, 24));
                sendMessagePanel.add(reqId);

                jLabel6.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel6.setText("Dest:");
                jLabel6.setPreferredSize(new java.awt.Dimension(56, 24));
                sendMessagePanel.add(jLabel6);

                destId.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                destId.setText("0");
                destId.setPreferredSize(new java.awt.Dimension(56, 24));
                sendMessagePanel.add(destId);

                jLabel7.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel7.setText("Message");
                jLabel7.setPreferredSize(new java.awt.Dimension(72, 24));
                sendMessagePanel.add(jLabel7);

                message.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                message.setText("Where are you?");
                message.setPreferredSize(new java.awt.Dimension(96, 24));
                sendMessagePanel.add(message);

                sendButton.setText("Send");
                sendButton.setPreferredSize(new java.awt.Dimension(96, 24));
                sendButton.addActionListener(new java.awt.event.ActionListener() {
                        public void actionPerformed(java.awt.event.ActionEvent evt) {
                                sendButtonActionPerformed(evt);
                        }
                });
                sendMessagePanel.add(sendButton);

                jPanel1.add(sendMessagePanel);

                statPanel.setBorder(javax.swing.BorderFactory.createBevelBorder(javax.swing.border.BevelBorder.RAISED));

                statField.setText("Stat");
                statField.setPreferredSize(new java.awt.Dimension(520, 24));
                statPanel.add(statField);

                jPanel1.add(statPanel);

                servicePanel.setBorder(javax.swing.BorderFactory.createBevelBorder(javax.swing.border.BevelBorder.RAISED));

                egLabel.setText("Stat");
                egLabel.setPreferredSize(new java.awt.Dimension(180, 24));
                servicePanel.add(egLabel);

                egButton.setText("Send");
                egButton.setPreferredSize(new java.awt.Dimension(96, 24));
                egButton.addActionListener(new java.awt.event.ActionListener() {
                        public void actionPerformed(java.awt.event.ActionEvent evt) {
                                egButtonActionPerformed(evt);
                        }
                });
                servicePanel.add(egButton);

                egTextField.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                egTextField.setText("12");
                egTextField.setToolTipText("No of Auto Testing Nodes");
                egTextField.setPreferredSize(new java.awt.Dimension(32, 24));
                servicePanel.add(egTextField);

                jPanel1.add(servicePanel);

                gridBagConstraints = new java.awt.GridBagConstraints();
                gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
                gridBagConstraints.insets = new java.awt.Insets(2, 0, 2, 0);
                getContentPane().add(jPanel1, gridBagConstraints);

                jScrollPane1.setPreferredSize(new java.awt.Dimension(452, 180));

                table.setModel(new javax.swing.table.DefaultTableModel(
                        new Object [][] {
                                {null, null, null, null},
                                {null, null, null, null},
                                {null, null, null, null},
                                {null, null, null, null}
                        },
                        new String [] {
                                "Title 1", "Title 2", "Title 3", "Title 4"
                        }
                ));
                table.setPreferredSize(new java.awt.Dimension(300, 150));
                jScrollPane1.setViewportView(table);

                gridBagConstraints = new java.awt.GridBagConstraints();
                gridBagConstraints.gridx = 0;
                gridBagConstraints.gridy = 1;
                gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
                gridBagConstraints.weightx = 1.0;
                gridBagConstraints.weighty = 1.0;
                gridBagConstraints.insets = new java.awt.Insets(2, 0, 2, 0);
                getContentPane().add(jScrollPane1, gridBagConstraints);

                statusField.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
                statusField.setText("Status...");
                statusField.setBorder(javax.swing.BorderFactory.createEtchedBorder());
                gridBagConstraints = new java.awt.GridBagConstraints();
                gridBagConstraints.gridx = 0;
                gridBagConstraints.gridy = 2;
                gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
                gridBagConstraints.insets = new java.awt.Insets(2, 0, 2, 0);
                getContentPane().add(statusField, gridBagConstraints);

                loggerControlPanel.setBorder(javax.swing.BorderFactory.createBevelBorder(javax.swing.border.BevelBorder.RAISED));

                jLabel1.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel1.setText("Node ID");
                jLabel1.setPreferredSize(new java.awt.Dimension(48, 20));
                loggerControlPanel.add(jLabel1);

                logNodeID.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                logNodeID.setPreferredSize(new java.awt.Dimension(32, 20));
                loggerControlPanel.add(logNodeID);

                jLabel2.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel2.setText("Type");
                jLabel2.setPreferredSize(new java.awt.Dimension(32, 20));
                loggerControlPanel.add(jLabel2);

                logType.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                logType.setPreferredSize(new java.awt.Dimension(32, 20));
                loggerControlPanel.add(logType);

                jLabel3.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel3.setText("Class");
                jLabel3.setPreferredSize(new java.awt.Dimension(36, 20));
                loggerControlPanel.add(jLabel3);

                logClass.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                logClass.setText("0");
                logClass.setPreferredSize(new java.awt.Dimension(32, 20));
                loggerControlPanel.add(logClass);

                jLabel4.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel4.setText("Location");
                jLabel4.setPreferredSize(new java.awt.Dimension(52, 20));
                loggerControlPanel.add(jLabel4);

                logLocation.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                logLocation.setText("4");
                logLocation.setPreferredSize(new java.awt.Dimension(32, 20));
                loggerControlPanel.add(logLocation);

                jLabel9.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
                jLabel9.setText("Contaions");
                jLabel9.setPreferredSize(new java.awt.Dimension(52, 20));
                loggerControlPanel.add(jLabel9);

                contatain.setHorizontalAlignment(javax.swing.JTextField.TRAILING);
                contatain.setPreferredSize(new java.awt.Dimension(64, 20));
                loggerControlPanel.add(contatain);

                printLogButton.setText("Print");
                printLogButton.setPreferredSize(new java.awt.Dimension(64, 24));
                printLogButton.addActionListener(new java.awt.event.ActionListener() {
                        public void actionPerformed(java.awt.event.ActionEvent evt) {
                                printLogButtonActionPerformed(evt);
                        }
                });
                loggerControlPanel.add(printLogButton);

                gridBagConstraints = new java.awt.GridBagConstraints();
                gridBagConstraints.gridy = 3;
                gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
                getContentPane().add(loggerControlPanel, gridBagConstraints);

                pack();
        }// </editor-fold>//GEN-END:initComponents

private void sendButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_sendButtonActionPerformed

//		testerController.send(message.getText(), this.destId.getText(), this.reqId.getText());
//                testerController.publishService(10);
    testerController.storeData();
}//GEN-LAST:event_sendButtonActionPerformed

private void startNodeSetButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_startNodeSetButtonActionPerformed
//		this.startNodeSetButton.setEnabled(false);
//		this.numOfTesters.setEnabled(false);
		Thread thread = new Thread(this);
		this.startNodeThreadId = thread.getId();
		thread.start();
		this.ringTestButton.setEnabled(true);
		this.autoTestButton.setEnabled(true);
}//GEN-LAST:event_startNodeSetButtonActionPerformed

private void autoTestButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_autoTestButtonActionPerformed
		this.autoTestButton.setEnabled(false);
		Thread thread = new Thread(this);
		this.startAutoTestThreadId = thread.getId();
		thread.start();
}//GEN-LAST:event_autoTestButtonActionPerformed

private void ringTestButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_ringTestButtonActionPerformed

		testerController.startRingTest();

}//GEN-LAST:event_ringTestButtonActionPerformed

private void printLogButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_printLogButtonActionPerformed

		testerController.printLogs(
				this.logNodeID.getText(),
				this.logType.getText(),
				this.logClass.getText(),
				this.logLocation.getText(),
				this.contatain.getText());


}//GEN-LAST:event_printLogButtonActionPerformed

private void egButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_egButtonActionPerformed
    testerController.retrieveData();
    
}//GEN-LAST:event_egButtonActionPerformed
		/**
		 * @param args the command line arguments
		 */
        // Variables declaration - do not modify//GEN-BEGIN:variables
        private javax.swing.JButton autoTestButton;
        private javax.swing.JPanel autoTestButtonPanel;
        private javax.swing.JTextField contatain;
        private javax.swing.JTextField destId;
        private javax.swing.JButton egButton;
        private javax.swing.JLabel egLabel;
        private javax.swing.JTextField egTextField;
        private javax.swing.JLabel jLabel1;
        private javax.swing.JLabel jLabel2;
        private javax.swing.JLabel jLabel3;
        private javax.swing.JLabel jLabel4;
        private javax.swing.JLabel jLabel5;
        private javax.swing.JLabel jLabel6;
        private javax.swing.JLabel jLabel7;
        private javax.swing.JLabel jLabel9;
        private javax.swing.JPanel jPanel1;
        private javax.swing.JScrollPane jScrollPane1;
        private javax.swing.JTextField logClass;
        private javax.swing.JTextField logLocation;
        private javax.swing.JTextField logNodeID;
        private javax.swing.JTextField logType;
        private javax.swing.JPanel loggerControlPanel;
        private javax.swing.JTextField message;
        private javax.swing.JTextField numOfAutoTesters;
        private javax.swing.JTextField numOfTesters;
        private javax.swing.JTextField portRange;
        private javax.swing.JButton printLogButton;
        private javax.swing.JTextField reqId;
        private javax.swing.JButton ringTestButton;
        private javax.swing.JButton sendButton;
        private javax.swing.JPanel sendMessagePanel;
        private javax.swing.JPanel servicePanel;
        private javax.swing.JButton startNodeSetButton;
        private javax.swing.JLabel statField;
        private javax.swing.JPanel statPanel;
        private javax.swing.JLabel statusField;
        private javax.swing.JTable table;
        // End of variables declaration//GEN-END:variables

		@Override
		public void run() {
				if (Thread.currentThread().getId() == this.startNodeThreadId) {
						testerController.startNodeSet(Integer.parseInt(this.portRange.getText()),
								Integer.parseInt(this.numOfTesters.getText()));
				} else if (Thread.currentThread().getId() == this.startAutoTestThreadId) {
						testerController.startAutoTest(Long.parseLong(this.numOfAutoTesters.getText()));
						this.autoTestButton.setEnabled(true);
				}
		}
}
