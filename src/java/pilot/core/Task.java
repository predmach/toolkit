package pilot.core;

import java.util.List;

import pilot.core.data.DataItem;

import bigs.api.core.Configurable;

public interface Task extends TextSerializable, Configurable {
	
	public List<TaskContainer<? extends Task>> getTaskContainerCascade();
}
