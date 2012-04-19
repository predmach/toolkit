package pilot.testing;

public abstract class GenericDataType {

	Integer value = 1;

	public Integer getValue() {
		return value;
	}

	public abstract void setValue(Integer value);
	
	public String toString() {
		return "value = "+value;
	}
}
