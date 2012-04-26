package bigs.api.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

public class LLDDataItem extends DataItem {

	List<List<Double>> data = new ArrayList<List<Double>>();

	public LLDDataItem() {}
	
	public LLDDataItem(List<List<Double>> data) {
		this.data = data;
	}
	
	public byte[] asFileContent() {
		return this.toTextRepresentation().getBytes();
	}
	
	public List<List<Double>> getLLD() {
		return this.data;
	}
	
	public void setLLD(List<List<Double>> data) {
		this.data = data;
	}
	
	public void addLD(List<Double> list) {
		data.add(list);
	}
	
	@Override
	public String toTextRepresentation() {
		String s = JSONValue.toJSONString(data);	
		return s;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		data = new ArrayList<List<Double>>();
		JSONArray o = (JSONArray)JSONValue.parse(textRepresentation);
		for (int i=0; i<o.size(); i++) {
			List<Double> ll = (List<Double>)o.get(i);
			data.add(ll);
		}			
	}

	@Override
	public Map<String, String> getMetadata() {
		return null;
	}

	@Override
	public void setMetadata(Map<String, String> metadata) {
	}

}
