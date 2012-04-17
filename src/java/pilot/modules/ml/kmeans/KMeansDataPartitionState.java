package pilot.modules.ml.kmeans;

import pilot.core.TextSerializable;

public class KMeansDataPartitionState implements TextSerializable {

	Integer processedPartitions = 0;
	
	@Override
	public String toTextRepresentation() {
		return this.getClass().getSimpleName()+ " processedPartitions="+this.processedPartitions;
	}

	@Override
	public void fromTextRepresentation(String textRepresentation) {
	}

}
