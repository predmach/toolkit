package bigs.api.core;

import bigs.api.storage.Put;
import bigs.api.storage.Result;

public abstract class State implements TextSerializable {

	public static State fromResultObject(Result result, String columnFamily, String classColumn, String objectColumn) {
		State r;
		byte[] sprocessStateClass = result.getValue(columnFamily, classColumn);		
		if (sprocessStateClass!=null) {
			Object obj;
			try {
				obj = Class.forName(new String(sprocessStateClass)).newInstance();
				if (! (obj instanceof TextSerializable)) {
					throw new BIGSException("state object for rowkey "+result.getRowKey()+" is not "+TextSerializable.class.getSimpleName());
				}
				
				r = (State)obj;
				byte[] sprocessStateObject = result.getValue(columnFamily, objectColumn);		
				if (sprocessStateObject!=null) {
					r.fromTextRepresentation(new String(sprocessStateObject));
				}
				return r;

			} catch (Exception e) {
				throw new BIGSException("error recreating state object "+e.getMessage());
			} 
		} else {
			return null;
		}
	}

	public void toPutObject (Put put, String columnFamily, String classColumn, String objectColumn) {
		put.add(columnFamily, classColumn, this.getClass().getName());
		put.add(columnFamily, objectColumn, this.toTextRepresentation());					
	}
}
