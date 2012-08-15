/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sinchana;

import java.util.Set;
import sinchana.thrift.DataObject;

/**
 *
 * @author Hiru
 */
public interface SinchanaServiceInterface {

    /**
     * 
     * @param dataObject which is published
     */
    public abstract void publish(DataObject dataObject);

    /**
     * 
     * @param dataObjectSet requested 
     */
    public abstract void get(Set<DataObject> dataObjectSet);

    /**
     * 
     * @param dataObject removed
     */
    public abstract void remove(DataObject dataObject);

    /**
     * 
     * @param success boolean value whether the dataObject published successfully
     */
    public abstract void isPublished(Boolean success);

    /**
     * 
     * @param success boolean value whether the dataObject removed successfully
     */
    public abstract void isRemoved(Boolean success);
    
}
