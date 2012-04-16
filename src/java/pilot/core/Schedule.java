package pilot.core;

import java.util.ArrayList;
import java.util.List;

public class Schedule {

	PipelineStage pipelineStage;
	List<ScheduleItem> items = new ArrayList<ScheduleItem>();
	
	public Schedule(PipelineStage pipelineStage) {
		this.pipelineStage = pipelineStage;
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
	
	public void save() {
		for (ScheduleItem item: items) {
			item.save();
		}
	}
}
