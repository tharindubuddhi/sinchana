/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import sinchana.thrift.DataObject;
import sun.security.util.BigInt;

/**
 *The class use to 
 * @author DELL
 */

public class SinchanaDataStore {

    private Map<String, Set<DataObject>> rootObjects = new HashMap<String, Set<DataObject>>();
    private Set<DataObject> storedObjects = new HashSet<DataObject>();
    private Server server = null;
    private Thread storeObjects = null;
    private Timer timer = null;
    private TimerTask timerTask = null;
    public static int timeOut = 10000;
    
    /**
     * 
     * @param server
     */
    public SinchanaDataStore(Server server) {
        this.server = server;
    }
    
    
    /**
     * 
     * @param objectKey HashKey of the data
     * @return the relevant Set for a given objectKey
     */
    public Set<DataObject> get(String objectKey) {
        return rootObjects.get(objectKey);
    }

    /**
     * 
     * @param key HashKey of the data
     * @param data 
     * @return
     */
    public boolean addStoreObjects(String key,String data){
        DataObject dataObject = new DataObject();
        dataObject.setDataKey(key);
        dataObject.setDataValue(data);
        dataObject.setSourceID(this.server.serverId);
        dataObject.setSourceAddress(this.server.address);
        storedObjects.add(dataObject);
        return true;
    }
    /**
     * Method add a dataObject to the Map with the objectKey
     * @param dataObject which need to be added to the Set
     * @return the success value of the storing action
     */
    public boolean store(DataObject dataObject) {
  
        Set<DataObject> sourceServers = rootObjects.get(dataObject.dataKey);
        if (sourceServers == null) {
            sourceServers = new HashSet<DataObject>();
        }
        boolean success = sourceServers.add(dataObject);
        rootObjects.put(dataObject.dataKey, sourceServers);
        storeObjects();
        return success;
    }

    /**
     * Method remove the particular dataObject from the Map and returns the removal is success or not
     * @param dataObject which needs to be removed from the Map
     * @return  the success value of the removal action
     */
    public boolean removeData(DataObject dataObject) {

        Set<DataObject> sourceServers = rootObjects.get(dataObject.dataKey);
        if (sourceServers == null) {
            System.out.println("no objects for that key");
        } else if (sourceServers.contains(dataObject)) {
            sourceServers.remove(dataObject);
            return true;
        } else {
            System.out.println("no objects published from that source for this object");
        }
        return false;
    }
    
    /**
     * Method which publish data time to time
     */
    public void storeObjects(){
        if(timer==null||timerTask==null){
            timer = new Timer();
            timerTask = new TimerTask() {

                @Override
                public void run() {
                    for (DataObject dataObject : storedObjects) {
                        server.storeData(dataObject.dataKey, dataObject.dataValue);
                    }
                    
                }
            };
            timer.schedule(timerTask,timeOut,timeOut);
        }        
    }
//    public void storeObjects(){
//        final int TIMEOUT = 5;
//        if(storeObjects==null){
//            storeObjects = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    int timeOut = 0;
//                    while(true){
//                        if(timeOut<TIMEOUT){
//                          timeOut++;
//                            try {
//                                Thread.sleep(1000);
//                            } catch (InterruptedException ex) {
//                                Logger.getLogger(SinchanaDataStore.class.getName()).log(Level.SEVERE, null, ex);
//                            }
//                        }else{
//                        if(storedObjects.size()==0){
//                            try {
//                                Thread.sleep(1000);
//                            } catch (InterruptedException ex) {
//                                Logger.getLogger(SinchanaDataStore.class.getName()).log(Level.SEVERE, null, ex);
//                            }
//                        }else{
//                        
//                        }
//                        
//                        }
//                        
//                    }
//                }
//            });
//        }     
//    }
}
