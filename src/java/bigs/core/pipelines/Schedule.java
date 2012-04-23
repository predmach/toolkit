package bigs.core.pipelines;

import java.util.ArrayList;
import java.util.List;

import bigs.api.storage.DataSource;
import bigs.api.storage.Result;
import bigs.api.storage.ResultScanner;
import bigs.api.storage.Scan;
import bigs.api.storage.Table;
import bigs.core.BIGS;

public class Schedule {

	PipelineStage pipelineStage;
	List<ScheduleItem> items = new ArrayList<ScheduleItem>();
	
	public Schedule(PipelineStage pipelineStage) {
		this.pipelineStage = pipelineStage;
	}
	
	public PipelineStage getPipelineStage() {
		return this.pipelineStage;
	}
	
	public void addItem(ScheduleItem item) {
		items.add(item);
		if (item.getId()==null) {
			item.setId(items.indexOf(item));			
		}
	}
	
	public List<ScheduleItem> getItems() {
		return items;
	}
	
	/**
	 * returns the item corresponding to the id passed as argument. It traverses the whole list
	 * searching for the item with the given number and, therefore, not assuming that items in
	 * the list are ordered by their id.
	 * @param index the index of the item to retreive
	 * @return the schedule item. NULL if none was found
	 */
	public ScheduleItem get(Integer id) {
		for (ScheduleItem si: this.items) {
			if (si.getId().equals(id)) {
				return si;
			}
		}
		return null;
	}
	
	/**
	 * saves this schedule in the underlying storage
	 */
	public void save() {
		for (ScheduleItem item: items) {
			item.save();
		}
	}
	
	/**
	 * loads the schedule corresponding to a pipeline stage from the underlying storage
	 * @param pipelineStage the pipelineStage for which to load the schedule
	 * @return the loaded schedule
	 */
	public static Schedule load(PipelineStage pipelineStage) {
		Schedule r = new Schedule(pipelineStage);
		DataSource dataSource = BIGS.globalProperties.getPreparedDataSource();

		ScheduleItem dummyStart = new ScheduleItem();
		dummyStart.schedule = r;
		dummyStart.setId(0);
				
		ScheduleItem dummyStop = new ScheduleItem();
		dummyStop.schedule = r;
		dummyStop.setId(new Integer(999999999));

		Table table = dataSource.getTable(ScheduleItem.tableName);
		Scan scan = table.createScanObject();
		scan.setStartRow(dummyStart.getRowKey());
		scan.setStopRow(dummyStop.getRowKey());
		
		ResultScanner rs = table.getScan(scan);
		
		try {
			for (Result rr = rs.next(); rr!=null; rr = rs.next()) {					
				ScheduleItem.fromResultObject(r, rr);
			}
		} finally {
			rs.close();
		}			
		return r;
	}
}
