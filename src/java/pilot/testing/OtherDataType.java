package pilot.testing;

public class OtherDataType extends GenericDataType {

	@Override
	public void setValue(Integer value) {
		this.value = value*3;
	}
	
	public String toString() {
		return "OT "+super.toString();
	}
}
