package pilot.testing;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import pilot.core.data.LLDDataItem;

public class Test {
	public static void run() {

		
		List<List<Double>> l = new ArrayList<List<Double>>();
		
		List<Double> l1 = new ArrayList<Double>();
		List<Double> l2 = new ArrayList<Double>();
		
		l1.add(1D);
		l1.add(2D);
		l1.add(3D);
		l2.add(100.12D);
		l2.add(98.8D);
		
		l.add(l1);
		l.add(l2);

		LLDDataItem item = new LLDDataItem();
		item.setLLD(l);
		
		String s = item.toTextRepresentation();
		
		System.out.println("to:   "+s);
		
		item.fromTextRepresentation(s);
		
		System.out.println("from: "+item.toTextRepresentation());
		
		
		GenericClass impl = new MyImplementation();
		GenericDataType data = new MyDataType();

		System.out.println(data.toString());		
		impl.doSomething(data);
		
		data = new OtherDataType();
		System.out.println(data.toString());		
//		impl.doSomething(data);

		Class<?> c = MyImplementation.class;

		Type g = c.getGenericSuperclass();
		
		System.out.println("generic class "+g.toString()+" ");

		if (g instanceof ParameterizedType) {
			ParameterizedType p = (ParameterizedType)g;
			for (Type t:p.getActualTypeArguments()) {
				System.out.println("type arg "+ t.toString());
			}
			System.out.println("owner "+p.getOwnerType());
			System.out.println("raw "+p.getRawType());
			
		}
	}
}
