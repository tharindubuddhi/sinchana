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
public interface SinchanaStoreInterface {

	public abstract void store(DataObject dataObject);

	public abstract void get(Set<DataObject> dataObjectSet);

	public abstract void remove(DataObject dataObject);

	public abstract void isStored(Boolean success);

	public abstract void isRemoved(Boolean success);
//                public abstract boolean store(String objectkey, String data);
//
//		public abstract void get(String sourceKey);
//
//		public abstract boolean remove(String objectkey, String sourcekey);
}
