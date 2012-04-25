package bigs.api.core;

import java.util.List;

import bigs.api.data.DataItem;

public interface Task extends TextSerializable, Configurable {
	
	public List<TaskContainer<? extends Task>> getTaskContainerCascade();

	public String getDescription();
}
