package bigs.api.examples.task;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSException;
import bigs.api.core.State;
import bigs.api.core.TextSerializable;


public class IterateAndSplitState extends State {

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
