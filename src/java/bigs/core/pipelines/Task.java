package bigs.core.pipelines;

import java.util.List;

import bigs.api.core.Configurable;
import bigs.api.data.DataItem;

public interface Task extends TextSerializable, Configurable {
	
	public List<TaskContainer<? extends Task>> getTaskContainerCascade();
	
}
