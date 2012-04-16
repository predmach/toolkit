package pilot.core;

public interface TextSerializable {
	public String toTextRepresentation();
	public TextSerializable fromTextRepresentation(String textRepresentation);
}
