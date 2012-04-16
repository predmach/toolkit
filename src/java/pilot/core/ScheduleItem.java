package pilot.core;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import bigs.api.exceptions.BIGSException;
import bigs.api.utils.TextUtils;
import bigs.core.utils.Core;
import bigs.core.utils.Text;

public class ScheduleItem {
	
	public final static String tableName = "schedules";
	public final static String[] columnFamilies = new String[]{ "params", "bigs" };	
	
	
	public static Integer NONE        = 0;
	public static Integer SCHEDULED   = 1;
	public static Integer IN_PROGRESS = 2;
	public static Integer DONE        = 3;
	public static Integer FAILED      = 4;
	
	static String[] statusStrings = new String[]{ "  NONE    ",
		                                          "SCHEDULED ",
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
	
	Integer status = ScheduleItem.NONE;
	
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
	
	public Boolean isStatusNone() {
		return status == ScheduleItem.NONE;
	}
	
	public Boolean isStatusScheduled() {
		return status == ScheduleItem.SCHEDULED;
	}
	
	public Boolean isStatusInProgress() {
		return status == ScheduleItem.IN_PROGRESS;
	}
	
	public Boolean isStatusDone() {
		return status == ScheduleItem.DONE;
	}
	
	public Boolean isStatusFailed() {
		return status == ScheduleItem.FAILED;				
	}
	
	public void setStatusNone() {
		status = ScheduleItem.NONE;
	}
	
	public void setStatusScheduled() {
		status = ScheduleItem.SCHEDULED;
	}

	public void setStatusInProgress() {
		status = ScheduleItem.IN_PROGRESS;
	}
	
	public void setStatusDone() {
		status = ScheduleItem.DONE;
	}

	public void setStatusFailed() {
		status = ScheduleItem.FAILED;
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
	 * this is the UUID of an evaluation retrieved from the DB that has not necessarily been created/updated
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
	 * this is the hostname of an evaluation retrieved from the DB that has not necessarily been created/updated
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
			}
		}
		throw new BIGSException("status "+statusString+" not recognized for a schedule item");
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
	 * returns as string representation of the rowkey corresponding to this evaluation
	 * @return
	 */
	public String getRowKey() {
		String explorationNumber = Text.zeroPad(new Long(this.schedule.pipelineStage.exploration.getExplorationNumber()), 5);
		String stageNumber = Text.zeroPad(new Long(this.schedule.pipelineStage.getStageNumber()),5);
		String scheduleItemNumber = Text.zeroPad(new Long(this.id), 5);	
		return explorationNumber + "." + stageNumber + "." + scheduleItemNumber;
	}
	
	public String toString() {
		String r = configuredTaskContainer.toString()+ " "+	configuredTask.toString() + " " + methodName;
		return r;
	}
	
}
