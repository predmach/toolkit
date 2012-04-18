package pilot.core.examples.task;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.exceptions.BIGSException;

import pilot.core.TextSerializable;

public class KMeansState implements TextSerializable {

	public Double value = 0D;
	
	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {
		JSONObject obj = new JSONObject();
		obj.put("value", this.value);
		return obj.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		JSONParser parser = new JSONParser();
		try {
			Map<String, Double> json = (Map<String, Double>)parser.parse(textRepresentation);
			if (json.get("value")!=null) this.value= json.get("value").doubleValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}

}
