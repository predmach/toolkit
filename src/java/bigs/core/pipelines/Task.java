package bigs.core.pipelines;

import java.util.List;

import bigs.api.core.Configurable;

public interface Task extends TextSerializable, Configurable {
	
	public List<TaskContainer<? extends Task>> getTaskContainerCascade();
}
