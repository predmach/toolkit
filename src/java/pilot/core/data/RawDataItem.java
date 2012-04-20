package pilot.core.data;

public class RawDataItem implements DataItem {

	byte[] data;
	
	public byte[] getBytes() {
		return this.data;
	}

	public void setBytes(byte[] data) {
		this.data = data;
	}
	
	@Override
	public String toTextRepresentation() {
		return new String(data);
	}

	@Override
	public void fromTextRepresentation(String textRepresentation) {
		data = textRepresentation.getBytes();
	}
	
}
