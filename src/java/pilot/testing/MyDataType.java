package pilot.testing;

public class MyDataType extends GenericDataType {

	Integer other = 10;
	
	@Override
	public void setValue(Integer value) {
		this.value = value * 2;
	}
	
	public String toString() {
		return "MY "+super.toString()+" other = "+other;
	}

}
