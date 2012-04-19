package pilot.testing;

public class MyImplementation extends GenericClass<MyDataType> {

	@Override
	public MyDataType processData(MyDataType item) {
		// TODO Auto-generated method stub
		return new MyDataType();
	}
	

	@Override
	public void doSomething(MyDataType item) {
		System.out.println("doing something with "+item);		
	}

}
