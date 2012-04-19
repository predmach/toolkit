package pilot.testing;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class Test {
	public static void run() {
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
