package test.bigs.core;

import bigs.api.exceptions.BIGSException;
import bigs.api.storage.DataSource;
import bigs.core.BIGS;
import bigs.core.BIGSProperties;
import bigs.core.pipelines.Pipeline;
import bigs.core.pipelines.ScheduleItem;
import bigs.core.utils.Data;
import bigs.core.utils.Log;

public class Utils {

	public static Boolean bigsTableInitialized = false;
	public static Boolean evaluationsTableInitialized = false;
	public static Boolean explorationsTableInitialized = false;
	public static Boolean globalPropertiesLoaded = false;
	
	public static void initializeBIGSTable() {
		if (bigsTableInitialized) return;
		DataSource dataSource = BIGS.globalProperties.getPreparedDataSource(BIGSProperties.DONOT_CREATE_TABLES);
		Data.createTableIfDoesNotExist(dataSource, BIGS.tableName, BIGS.columnFamilies);
		bigsTableInitialized = true;
	}
	
	public static void initializeExplorationsTable() {
		if (explorationsTableInitialized) return ;		
		DataSource dataSource = BIGS.globalProperties.getPreparedDataSource(BIGSProperties.DONOT_CREATE_TABLES);
		if (dataSource.existsTable(Pipeline.tableName)) {
			throw new BIGSException("must delete table "+Pipeline.tableName+" before starting tests");
		}
		Data.createTableIfDoesNotExist(dataSource, Pipeline.tableName, Pipeline.columnFamilies);
		explorationsTableInitialized = true;
	}
	
	public static void initializeEvaluationsTable() {
		if (evaluationsTableInitialized) return ;		
		DataSource dataSource = BIGS.globalProperties.getPreparedDataSource(BIGSProperties.DONOT_CREATE_TABLES);
		if (dataSource.existsTable(ScheduleItem.tableName)) {
			Log.info("table "+ScheduleItem.tableName+" exists. Please delete it before staring tests");
			throw new BIGSException("must delete table "+ScheduleItem.tableName+" before starting tests");
		}
		Data.createTableIfDoesNotExist(dataSource, ScheduleItem.tableName, ScheduleItem.columnFamilies);
		evaluationsTableInitialized = true;
	}
}
