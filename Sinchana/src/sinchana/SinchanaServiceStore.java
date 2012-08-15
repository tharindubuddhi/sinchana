/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import sinchana.thrift.DataObject;

/**
 *
 * @author DELL
 */
public class SinchanaServiceStore {
    
    private Map<String, Set<DataObject>> rootServices = new HashMap<String, Set<DataObject>>();
    private Set<DataObject> publishedServices = new HashSet<DataObject>();

    /**
     * 
     * @return the relevant Set for a given serviceKey
     */
    public Set<DataObject> get(String serviceKey) {
        return rootServices.get(serviceKey);
    }

    /**
     * Method add a dataObject to the Map with the serviceKey
     * @param dataObject which need to be added to the Set
     * @return the success value of the storing action
     */
    public boolean publishService(DataObject dataObject) {

        Set<DataObject> sourceServers = rootServices.get(dataObject.dataKey);
        if (sourceServers == null) {
            sourceServers = new HashSet<DataObject>();
        }

        boolean success = sourceServers.add(dataObject);
        rootServices.put(dataObject.dataKey, sourceServers);
        return success;
    }

    /**
     * Method remove the particular dataObject from the Map and returns the removal is success or not. 
     * @param dataObject which needs to be removed from the Map
     * @return  the success value of the removal action
     */
    public boolean removeService(DataObject dataObject) {

        Set<DataObject> sourceServers = rootServices.get(dataObject.dataKey);
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
}
