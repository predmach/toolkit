package pilot.core;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;
import bigs.api.storage.DataSource;
import bigs.api.storage.Get;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.api.storage.Table;
import bigs.api.utils.TextUtils;
import bigs.core.BIGS;
import bigs.core.utils.Core;
import bigs.core.utils.Data;
import bigs.core.utils.Text;

public class ScheduleItem {
	
	public final static String tableName = "schedules";
	public final static String[] columnFamilies = new String[]{ "scheduling", "bigs", "content" };	
	
	
	public static Integer STATUS_PENDING     = 0;
	public static Integer STATUS_INPROGRESS  = 1;
	public static Integer STATUS_DONE        = 2;
	public static Integer STATUS_FAILED      = 3;
	
	static String[] statusStrings = new String[]{ " PENDING  ",
		                                          "INPROGRESS",
		                                          "   DONE   ",
		                                          "  FAILED  " };
	
	String methodName;
	List<Integer> parentsIds = new ArrayList<Integer>();
	Schedule schedule;
	Integer id;

	Date lastUpdate  = new Date(Core.getTime());
	Long elapsedTime = 0L;
	
	String uuiStored = "";
	String hostnameStored = "";

	Task configuredTask;
	TaskContainer configuredTaskContainer;
	
	Integer status = ScheduleItem.STATUS_PENDING;
	
	TextSerializable processState;
	
	public ScheduleItem() {}
	
	public ScheduleItem(Schedule schedule) {
		this.schedule = schedule;
		schedule.addItem(this);
	}
				
	public ScheduleItem (Schedule schedule, TaskContainer configuredTaskContainer, Task configuredTask, String methodName) {
		this(schedule);
		this.configuredTask = configuredTask;
		this.methodName = methodName;
		this.configuredTaskContainer = configuredTaskContainer;
	}
	
	public Integer getId() {
		return this.id;
	}
	
	public void setId(Integer id) {
		this.id = id;
	}
	
	public Schedule getSchedule() {
		return schedule;
	}
	
	public TextSerializable getProcessState() {
		return this.processState;
	}
	
	public void setProcessState(TextSerializable processState) {
		this.processState = processState;
	}
	
	public Task getConfiguredTask() {
		return this.configuredTask;
	}
	
	public TaskContainer getConfiguredTaskContainer() {
		return this.configuredTaskContainer;
	}
	
	public String getMethodName() {
		return this.methodName;
	}	
	
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	
	public Boolean isStatusPending() {
		return status == ScheduleItem.STATUS_PENDING;
	}
	
	public Boolean isStatusInProgress() {
		return status == ScheduleItem.STATUS_INPROGRESS;
	}
	
	public Boolean isStatusDone() {
		return status == ScheduleItem.STATUS_DONE;
	}
	
	public Boolean isStatusFailed() {
		return status == ScheduleItem.STATUS_FAILED;				
	}
	
	public void setStatusPending() {
		status = ScheduleItem.STATUS_PENDING;
	}

	public void setStatusInProgress() {
		status = ScheduleItem.STATUS_INPROGRESS;
	}
	
	public void setStatusDone() {
		status = ScheduleItem.STATUS_DONE;
	}

	public void setStatusFailed() {
		status = ScheduleItem.STATUS_FAILED;
	}

	public Integer getStatus() {
		return status;
	}
	
	public Date getLastUpdate() {
		return lastUpdate;
	}
	
	public void setLastUpdate(Date lastUpdate) {
		this.lastUpdate = lastUpdate;
	}
	
	public void setLastUpdateFromString(String lastUpdateString) {
		if (lastUpdateString==null || lastUpdateString.isEmpty()) {
			lastUpdate = null;
			return;
		}		try {
			lastUpdate = TextUtils.FULLDATE.parse(lastUpdateString);
		} catch (ParseException e) {
			throw new BIGSException("error parsing date "+lastUpdateString);
		}
	}

	public void setUuidStored(String uuidStored) {
		this.uuiStored = uuidStored;
	}
	
	/**
	 * this is the UUID of a schedule item retrieved from the DB that has not necessarily been created/updated
	 * by this process.
	 * @return
	 */
	public String getUuidStored() {
		return this.uuiStored;
	}
	
	public void setHostnameStored (String hostnameStored) {
		this.hostnameStored = hostnameStored;
	}

	/**
	 * this is the hostname of a schedule item retrieved from the DB that has not necessarily been created/updated
	 * by this process.
	 * @return
	 */
	public String getHostnameStored( ) {
		return this.hostnameStored;
	}
	public Long getElapsedTime() {
		return elapsedTime;
	}
	
	public void setElapsedTime(Long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}
	
	public void addToElapsedTime(Long time) {
		this.elapsedTime = this.elapsedTime + time;
	}
	
	public String getStatusAsString() {
		return ScheduleItem.statusStrings[this.status];
	}
	
	public void setStatusFromString(String statusString) {
		for (int i=0; i<ScheduleItem.statusStrings.length; i++) {
			if (statusString.trim().equals(ScheduleItem.statusStrings[i].trim())) {
				this.status = i;
				return;
			}
		}
		throw new BIGSException("status "+statusString.trim()+" not recognized for a schedule item");
	}

	public ScheduleItem addParentId(Integer parentId) {
		if (parentId!=null) {
			this.parentsIds.add(parentId);
		}
		return this;
	}
	
	public ScheduleItem addParentsIds (List<Integer> parentsIds) {
		for (Integer i: parentsIds) this.parentsIds.add(i);
		return this;
	}
	
	public List<Integer> getParentsIds() {
		return this.parentsIds;
	}
	
	/**
	 * returns as string representation of the rowkey corresponding to this schedule item
	 * @return
	 */
	public String getRowKey() {
		String explorationNumber = Text.zeroPad(new Long(this.schedule.pipelineStage.pipeline.getPipelineNumber()), 5);
		String stageNumber = Text.zeroPad(new Long(this.schedule.pipelineStage.getStageNumber()),5);
		String scheduleItemNumber = Text.zeroPad(new Long(this.id), 5);	
		return explorationNumber + "." + stageNumber + "." + scheduleItemNumber;
	}
	

	/**
	 * builds an empty SchedueItem object from the string representation of its rowkey.
	 * Leaves the task and task container (null)
	 * @param k
	 * @return
	 */
	static public ScheduleItem fromRowKey(Schedule schedule, String k) {	
		
		String[] s = k.split("\\.");
		if (s.length!=3) {
			throw new BIGSException("incorrect schedule item rowkey specification");
		}
		Integer pipelineNumber = new Integer(s[0]);
		Integer stageNumber = new Integer(s[1]);
		Integer scheduleItemNumber = new Integer(s[2]);
		if (!schedule.pipelineStage.stageNumber.equals(stageNumber) ||
			!schedule.pipelineStage.pipeline.getPipelineNumber().equals(pipelineNumber)) {
			throw new BIGSException("error reconstructing schedule item rowkey "+k+" does not correspond to exploration "+pipelineNumber+" / stage "+stageNumber);			
		}
		
		ScheduleItem r = new ScheduleItem(schedule);
		r.setId(scheduleItemNumber);
		return r;
	}
	
	/**
	 * builds an ScheduleItem object from a Result object obtained from a Scan or
	 * Get in the underlying storage
	 * @param result
	 * @return
	 */
	static public ScheduleItem fromResultObject(Schedule schedule, Result result) {
		ScheduleItem r = ScheduleItem.fromRowKey(schedule, result.getRowKey());
		
		
		byte[] stat = result.getValue("bigs", "status");
		if (stat!=null) {
			r.setStatusFromString(new String(stat));
		}
		
		byte[] etime = result.getValue("bigs", "elapsedtime");
		if (etime!=null) {
			r.setElapsedTime(new Long(new String(etime)));
		}

		byte[] lastupdate = result.getValue("bigs", "lastupdate");
		if (lastupdate!=null) {
			r.setLastUpdateFromString(new String(lastupdate));
		}
		
		byte[] suuid = result.getValue("bigs", "uuid");
		if (suuid!=null) {
			r.setUuidStored(new String(suuid));
		}

		byte[] smethod = result.getValue("scheduling", "method");
		if (smethod!=null) {
			r.setMethodName(new String(smethod));
		}

		byte[] shostname = result.getValue("bigs", "hostname");		
		if (shostname!=null) {
			r.setHostnameStored(new String(shostname));
		}
				
		byte[] sprocessStateClass = result.getValue("content", "class");		
		if (sprocessStateClass!=null) {
			Object obj;
			try {
				obj = Class.forName(new String(sprocessStateClass)).newInstance();
				if (! (obj instanceof TextSerializable)) {
					throw new BIGSException("state object for rowkey "+result.getRowKey()+" is not "+TextSerializable.class.getSimpleName());
				}
				
				TextSerializable processState = (TextSerializable)obj;
				byte[] sprocessStateObject = result.getValue("content", "data");		
				if (sprocessStateObject!=null) {
					processState.fromTextRepresentation(new String(sprocessStateObject));
				}
				r.setProcessState(processState);

			} catch (Exception e) {
				throw new BIGSException("error recreating state object "+e.getMessage());
			} 
		}		

		String parentsIdsString = new String(result.getValue("scheduling", "parents"));
		
		if (parentsIdsString!=null && !parentsIdsString.trim().isEmpty()) {
			r.parentsIds = Text.parseObjectList(parentsIdsString, " ", Integer.class);
		}
		
		byte[] taskClassNameBytes = result.getValue("scheduling", "task.class");
		if (taskClassNameBytes!=null) {
			Object obj = null;
			try {
				obj = Class.forName(new String(taskClassNameBytes)).newInstance();
			} catch (Exception e) {
				throw new BIGSException("error getting task in schedule item "+result.getRowKey()+". "+e.getMessage());
			}
			if (! (obj instanceof Task)) {
				throw new BIGSException("task schedule item in db with rowkey '"+result.getRowKey()+"' must instantiate "+Task.class.getName());
			}
			
			r.configuredTask = (Task)obj;
			byte[] taskObject = result.getValue("scheduling", "task.object");
			if (taskObject!=null) {
				r.configuredTask.fromTextRepresentation(new String(taskObject));
			}
		}		

		byte[] taskContainerClassNameBytes = result.getValue("scheduling", "task.container.class");
		if (taskContainerClassNameBytes!=null) {
			Object obj = null;
			try {
				obj = Class.forName(new String(taskContainerClassNameBytes)).newInstance();
			} catch (Exception e) {
				throw new BIGSException("error getting task conatiner in schedule item "+result.getRowKey()+". "+e.getMessage());
			}
			if (! (obj instanceof TaskContainer)) {
				throw new BIGSException("task container schedule item in db with rowkey '"+result.getRowKey()+"' must instantiate "+TaskContainer.class.getName());
			}
			
			r.configuredTaskContainer = (TaskContainer)obj;
			byte[] taskContainerObject = result.getValue("scheduling", "task.container.object");
			if (taskContainerObject!=null) {
				r.configuredTaskContainer.fromTextRepresentation(new String(taskContainerObject));
			}
		}		
		return r;
	}
	
	/**
	 * Fills a Put object leaving it ready to persist this schedule item into the underlying storage
	 * @param put the Put object to fill in
	 * @return the same object filled in
	 */
	public Put fillPutObject (Put put) {		

		if (this.configuredTask!=null) {
			put.add("scheduling", "task.class", this.configuredTask.getClass().getName());
			put.add("scheduling", "task.object", this.configuredTask.toTextRepresentation());			
		}
		
		if (this.configuredTaskContainer!=null) {
			put.add("scheduling", "task.container.class", this.configuredTaskContainer.getClass().getName());
			put.add("scheduling", "task.container.object", this.configuredTaskContainer.toTextRepresentation());			
		}

		put.add("bigs","status", Bytes.toBytes(this.getStatusAsString()));
		
		put.add("scheduling", "parents", Bytes.toBytes(Text.collate(this.parentsIds.toArray(), " ")));
		
		put.add("scheduling", "method", Bytes.toBytes(this.getMethodName()));
		
		if (this.getProcessState()!=null) {
			put.add("content", "class", Bytes.toBytes(this.getProcessState().getClass().getName()));
			
			put.add("content", "data", Bytes.toBytes(this.getProcessState().toTextRepresentation()));
		}
		
		lastUpdate = new Date(Core.getTime());
    	
		put.add("bigs", "lastupdate", Bytes.toBytes(TextUtils.FULLDATE.format(this.lastUpdate)));
    		
    	if (this.elapsedTime!=null) {
    		put.add("bigs", "elapsedtime", Bytes.toBytes(this.elapsedTime.toString()));
    	}
		return put;
	}
	
	/**
	 * Fills a Get object leaving it ready to completely retrieve this schedule item object from the underlying storage
	 * @param get the Get object to fill in
	 * @return the same Get object filled in
	 */
	public static Get fillGetObject (Get get) {
		
		for (String family: ScheduleItem.columnFamilies) {
			get.addFamily(family);
		}
		get.addColumn("bigs", "timestart");
		get.addColumn("bigs", "timedone");
		get.addColumn("bigs", "lastupdate");
		get.addColumn("bigs", "hostname");
		get.addColumn("bigs", "uuid");
		get.addColumn("scheduling", "task.class");
		get.addColumn("scheduling", "task.object");
		get.addColumn("scheduling", "task.container.object");
		get.addColumn("scheduling", "task.container.object");
		get.addColumn("scheduling", "method");
		get.addColumn("scheduling", "parents");
		get.addColumn("content", "data");
		get.addColumn("content", "class");
		return get;
	}	

	/**
	 * Loads an schedule item from a datasource
	 * 
	 * @param dataSource the datasource object where to look up the schedule item
	 * @param rowKey the rowkey of the schedule item to retrieve
	 * @return
	 */
	static public ScheduleItem load (DataSource dataSource, Schedule schedule, String rowKey) {
		Table table = dataSource.getTable(ScheduleItem.tableName);
		Get get = ScheduleItem.fillGetObject(table.createGetObject(rowKey));
		Result result = table.get(get);
		ScheduleItem r = ScheduleItem.fromResultObject(schedule, result);
		return r;
	}
	
	/**
	 * saves this schedule item in the underlying datasource
	 * @param dataSource the datasource
	 */
	public void save() {
    	DataSource dataSource = BIGS.globalProperties.getConfiguredDataSource();
    	Table table = dataSource.getTable(ScheduleItem.tableName);
    	table.put(Data.fillInHostMetadata(this.fillPutObject(table.createPutObject(this.getRowKey()))));				
	}
		
	/**
	 * time stamps this schedule item in the underlying storage by udpating the content
	 * of the column 'bigs:alive' with the current date only if the existing uuid is the
	 * same of this process.
	 * 
	 * @param dataSource the datasource object where this schedule item is stored
	 * @return true if the update was successful, false otherwise (if the existing uuid
	 *         is different, meaning that somebody else is working on this schedule item)
	 */
	public Boolean markAlive(DataSource dataSource) {
		Table table = dataSource.getTable(ScheduleItem.tableName);
		Put put = table.createPutObject(this.getRowKey());
		this.lastUpdate = new Date(Core.getTime());
		put = this.fillPutObject(put);
		put = Data.fillInHostMetadata(put);		
		Boolean r = table.checkAndPut(this.getRowKey(), "bigs", "uuid", Core.myUUID.getBytes(), put);
		return r;
	}
	
	
	public ScheduleItem clone() {
		ScheduleItem r = new ScheduleItem(this.schedule);
		r.configuredTask = this.configuredTask;
		r.configuredTaskContainer = this.configuredTaskContainer;
		r.id = this.id;
		r.parentsIds = this.parentsIds;
		r.hostnameStored = this.hostnameStored;
		r.uuiStored = this.uuiStored;
		r.lastUpdate = this.lastUpdate;
		r.status = this.status;
		r.elapsedTime = this.elapsedTime;
		r.processState = this.processState;
		return r;
	}
	
	Boolean bothNullOrEqual(Object o1, Object o2) {
		if (o1==null && o2==null) return true;
		if (o1==null || o2==null) return false;
		if (!o1.equals(o2)) return false;
		return true;
	}
	
	public Boolean equals(ScheduleItem si) {
		if (!bothNullOrEqual(this.schedule.pipelineStage.stageNumber, si.schedule.pipelineStage.stageNumber)) return false;
		if (!bothNullOrEqual(this.schedule.pipelineStage.pipeline.getPipelineNumber(), si.schedule.pipelineStage.pipeline.getPipelineNumber())) return false;

		if (!bothNullOrEqual(this.status, si.status)) return false;
		if (!bothNullOrEqual(this.elapsedTime, si.elapsedTime)) return false;
		
		return true;
	}		
	
	/**
	 * Checks if the schedule item is available to start working on it.
	 * Returns true if all the schedule item parents are done
	 */
	public boolean canProcess() {
		if (this.isStatusPending()) {
			Boolean anyParentNotFinished = false;
			for (Integer parentId: this.getParentsIds()) {
				ScheduleItem parent = this.getSchedule().get(parentId);
				if (parent==null) {
					throw new BIGSException("parent for schedule item "+this.getRowKey()+" not found");
				}
				if (!parent.isStatusDone()) {
					anyParentNotFinished = true;
					break;
				}
			}
			
			return !anyParentNotFinished;		
		}
		return false;
	}
		
	
	public String toString() {
		String r = this.getRowKey()+" "+configuredTaskContainer.toString()+ " "+	configuredTask.toString() + " " + methodName;
		return r;
	}
	
}
