package pilot.core.data;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

public class LLDDataItem implements DataItem {

	List<List<Double>> data = new ArrayList<List<Double>>();

	public List<List<Double>> getLLD() {
		return this.data;
	}
	
	public void setLLD(List<List<Double>> data) {
		this.data = data;
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

}
