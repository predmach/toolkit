package bigs.api.data;

import java.util.Map;

import bigs.api.core.BIGSException;
import bigs.api.core.TextSerializable;
import bigs.api.storage.DataSource;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.api.storage.Table;
import bigs.core.utils.Core;
import bigs.core.utils.Data;
import bigs.core.utils.Network;

public abstract class DataItem implements TextSerializable {

	public static String[] dataTableColumnFamilies = new String[] {"bigs", "content", "metadata", "tags"};

	static public void createDataTableIfDoesNotExist (DataSource dataSource, String tableName) {
		Data.createTableIfDoesNotExist(dataSource, tableName, dataTableColumnFamilies);
	}
	
	String rowkey = "";
	String hostName = "";
	String uuid = "";
	
	public abstract Map<String, String> getMetadata();

	public abstract void setMetadata(Map<String, String> metadata);
	
	public abstract byte[] asFileContent();
	
	public void setRowkey(String rowkey){
		this.rowkey = rowkey;
	}
	
	public String getRowkey() {
		return this.rowkey;
	}
	
	public void save(DataSource dataSource, String tableName) {
		Table table = dataSource.getTable(tableName);
		Put   put   = table.createPutObject(this.getRowkey());
		put.add("bigs", "hostname", Network.getHostName().getBytes());
		put.add("bigs", "uuid", Core.myUUID.getBytes());

		put.add("content", "data", this.toTextRepresentation().getBytes());
		put.add("content", "class", this.getClass().getName());
		
		
		Map<String, String> metadata = this.getMetadata();
		if (metadata!=null) {
			for (String k: metadata.keySet()) {
				String v = metadata.get(k);
				put.add("metadata", k, v);
			}
		}
		
		table.put(put);		
	}	
	
	public static DataItem fromResult(Result result) {
		return DataItem.fromResult(result, DataItem.FULL_OBJECT);
	}
	
	public static Integer EMPTY_OBJECT = 1;
	public static Integer FULL_OBJECT = 2;
	public static DataItem fromResult(Result result, Integer retrieveObject) {
		
		byte[] classNameBytes = result.getValue("content", "class");
		if (classNameBytes==null) {
			throw new BIGSException("object class spec not found in rowkey "+result.getRowKey());
		}

		String className = new String(classNameBytes);
		DataItem r = null;
		try {
			r = (DataItem)Class.forName(className).newInstance();
		} catch (Exception e) {
			throw new BIGSException("cannot create DataItem of type "+className+". "+e.getMessage());
		}

		if (retrieveObject.equals(FULL_OBJECT)) {
			byte[] objAsTextBytes = result.getValue("content", "data");
			if (objAsTextBytes!=null) r.fromTextRepresentation(new String(objAsTextBytes));
			
			byte[] hostnameBytes  = result.getValue("bigs", "hostname");
			if (hostnameBytes!=null) r.hostName = new String(hostnameBytes);
			
			byte[] uuidBytes      = result.getValue("bigs", "uuid");
			if (uuidBytes!=null) r.uuid = new String(uuidBytes);
			
			Map<String, String> metadata = result.getFamilyMap("metadata");
			r.setMetadata(metadata);
		}
		
		return r; 
	}
	
	
}
