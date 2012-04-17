package pilot.modules.ml.kmeans;

import pilot.core.TextSerializable;

public class KMeansIterationState implements TextSerializable {

	public Integer dummyParameter = 1;
	
	@Override
	public String toTextRepresentation() {
		return "dummyParameter = "+dummyParameter;
	}

	@Override
	public void fromTextRepresentation(String textRepresentation) {
	}

}
