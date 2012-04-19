package pilot.testing;

public abstract class GenericClass<T extends GenericDataType> {
	
	public abstract T processData(T item);
	
	public abstract void doSomething(T item);
}
