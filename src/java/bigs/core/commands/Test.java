package bigs.core.commands;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import bigs.api.core.Algorithm;
import bigs.api.data.DataItem;
import bigs.api.examples.task.IterateAndSplit;
import bigs.api.exceptions.BIGSException;
import bigs.api.storage.DataSource;
import bigs.api.storage.Get;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.api.storage.ResultScanner;
import bigs.api.storage.Scan;
import bigs.api.storage.Table;
import bigs.core.BIGS;
import bigs.core.pipelines.Pipeline;
import bigs.core.pipelines.PipelineStage;
import bigs.core.pipelines.Schedule;
import bigs.core.pipelines.ScheduleItem;
import bigs.core.utils.Log;
import bigs.core.utils.Text;
import bigs.modules.storage.dynamodb.DynamoDBDataSource;

import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;


public class Test extends Command {

	@Override
	public String getCommandName() {
		return "test";
	}

	@Override
	public String[] getHelpString() {
		return new String[] {
			"command for testing stuff"
		};
	}

    @Override
    public Boolean checkCallingSyntax(String[] args) {
    	return true;
    }

    @Override
	public void run(String[] args) throws Exception {
    	//this.testTextSerializable(args);
    	testGenerics(args);
    }
    
    void testGenerics(String[] args) {
    	this.testMultiFileer(args);
    }
    
    void testMultiFileer(String[] args) {
    	Pipeline pipeline = Pipeline.fromPipelineNumber(new Integer(args[0]));
    	PipelineStage stage = pipeline.getStages().get(0);
    	
		DataSource inputDataSource    = stage.getPreparedInputDataSource();
		String     inputDataTableName = stage.getInputTableName();
		Table 	   inputTable = inputDataSource.getTable(inputDataTableName);
				
		Scan scan = inputTable.createScanObject();
		for (String family: DataItem.dataTableColumnFamilies) {
			scan.addFamily(family);
		}

		Map<String, String> tags = new HashMap<String, String>();
		tags.put("00003.00001|IterativeTaskContainer|iteration", "1");
		tags.put("00003.00001|DataPartitionTaskContainer|partition", "1");
		
		if (tags!=null) {
			Log.debug("worker on data items with tag");
			for (String tagName: tags.keySet()) {
				String tagValue = tags.get(tagName);
				scan.addFilterByColumnValue("tags", tagName, tags.get(tagName).getBytes());
				Log.debug("adding filter "+tagName+" = "+tagValue);						
			}					
		}
		
		ResultScanner rs = inputTable.getScan(scan);
		
		try {
			for (Result rr = rs.next(); rr!=null; rr = rs.next()) {						
				Log.info("      input  rowkey "+rr.getRowKey());
			}
		} finally {
			rs.close();
		}
    	
    	
    }
    
    void testTextSerializable(String[] args) {
    	IterateAndSplit k = new IterateAndSplit();
    	k.numberOfIterations = 4;
    	k.numberOfPartitions = 5;
    	
    	System.out.println(k.toTextRepresentation());
    	
    	IterateAndSplit k2 = new IterateAndSplit();
    	k2.fromTextRepresentation(k.toTextRepresentation());
    	System.out.println(k2.toTextRepresentation());
    }
    
    void testPipelineProperties(String[] args) throws Exception {
    	Properties props = new Properties();
    	props.load(new FileReader(new File(args[0])));
    	
    	Pipeline pipeline = new Pipeline();
    	pipeline.load(new FileReader(new File(args[0])));
    	pipeline.save();
    	
    	PipelineStage p = pipeline.getStages().get(0);
    	
    	p.printOut();
System.out.println();    	
System.out.println();    	
    	Schedule schedule = p.generateSchedule();
    	
     	schedule.save();
System.out.println();    	
System.out.println();    	
		for (ScheduleItem si: schedule.getItems()) {
			String parents = "";
			for (Integer pi: si.getParentsIds()) {
				parents = parents + pi ;
				if (pi!=si.getParentsIds().get(si.getParentsIds().size()-1)) {
					parents = parents + " ";
				}
			}

			String prefix = "";
			System.out.println(Text.zeroPad(new Long(si.getId()),3)+" "
		                      +prefix+  " ("+parents+") " +si.toString());
		}
    	
System.out.println();    	
System.out.println("------- LOADING FROM STORAGE -------");    	
System.out.println();    	
		
		Pipeline p2 = Pipeline.fromPipelineNumber(pipeline.getPipelineNumber());
		PipelineStage p2s = p2.getStages().get(0);
		Schedule p2ss = p2s.loadSchedule();
		
		for (ScheduleItem si: p2ss.getItems()) {
			String parents = "";
			for (Integer pi: si.getParentsIds()) {
				parents = parents + pi ;
				if (pi!=si.getParentsIds().get(si.getParentsIds().size()-1)) {
					parents = parents + " ";
				}
			}

			String prefix = "";
			System.out.println(Text.zeroPad(new Long(si.getId()),3)+" "
		                      +prefix+  " ("+parents+") " +si.toString());
		}
    }

    
    void testDynamo(String[] args) throws Exception {
    	if (args.length==0) throw new BIGSException("must specify extra args");
    	DynamoDBDataSource d = (DynamoDBDataSource)BIGS.globalProperties.getPreparedDataSource();
    	String tableName = "test-table";
    	
    	if (args[0].equals("create.table")) {
			KeySchema keySchema = new KeySchema()
			.withHashKeyElement(new KeySchemaElement().withAttributeName("hkey").withAttributeType("S"))
			.withRangeKeyElement(new KeySchemaElement().withAttributeName("rkey").withAttributeType("S"));

	        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
	                .withKeySchema(keySchema)
	                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(3L).withWriteCapacityUnits(5L));
	        d.getAmazonDynamoDBClient().createTable(createTableRequest);
    		
    	} else if (args[0].equals("fill.table")) {
    		for (Integer hkey=1; hkey<20; hkey++) {
    			for (Integer rkey=1; rkey<20; rkey++) {
    				Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    				item.put("hkey", new AttributeValue().withS(hkey.toString()));
    				item.put("rkey", new AttributeValue().withS(rkey.toString()));
    				item.put("field1", new AttributeValue().withS(new Date().toString()));
    				item.put("field2", new AttributeValue().withS(new Date().toString()));
    				PutItemRequest putItemRequest = new PutItemRequest()
    				  .withTableName(tableName)
    				  .withItem(item);
    				PutItemResult result = d.getAmazonDynamoDBClient().putItem(putItemRequest);
    				Log.info("put "+result.toString());
    			}
    			
    		}
    	} else if (args[0].equals("scan")) {

	    	ScanResult result;

	    	Key lastKey = null;
	    	int i=1;
	    	do {
	    		ScanRequest scanRequest = new ScanRequest()
	    			.withTableName(tableName)
	    			.withLimit(1000);
		    	if (lastKey!=null) scanRequest.withExclusiveStartKey(lastKey);
		    	result = d.getAmazonDynamoDBClient().scan(scanRequest);
		    	for (Map<String, AttributeValue> item : result.getItems()) {
	    			StringBuffer sb = new StringBuffer();
		    		for (String fieldName: item.keySet()) {
		    			sb.append(fieldName).append("=").append(item.get(fieldName).getS()).append(" ");
		    		}	    		
		    		Log.info(i+"  "+sb.toString());
		    		i++;
		    	}  
		    	lastKey = result.getLastEvaluatedKey();
	    	} while (lastKey!=null);
    	}
    }

    void testS3(String[] args) {
       String accessKey = "AKIAIOWFQRUK2AT6FPNA";    // example accessKey
       String secretKey = "EFz4K0HCSV8zcDWgj6GBlwdbH9C/nt4mvs46OQaL";    // example secretKey
       AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
       AmazonS3 s3 = 
           new AmazonS3Client(credentials, 
        		   new ClientConfiguration().withProtocol(Protocol.HTTP));
       
       String bucketName = "rramos";    // example bucketName
       String key = "folder/";
       Log.debug("creating folder in bucket");
       InputStream input = new ByteArrayInputStream(new byte[0]);
       ObjectMetadata metadata = new ObjectMetadata();
       metadata.setContentLength(0);
       s3.putObject(new PutObjectRequest(bucketName, key, input, metadata));    	
       Log.debug("done");
       
       
    }

    void testExplorations(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    	DataSource d = BIGS.globalProperties.getPreparedDataSource();
    	
    	Table table = d.getTable(Pipeline.tableName);
    	Scan scan  = table.createScanObject();    	
    	scan.setStartRow("00002");
    	scan.setStopRow("00007");
    	scan.addFilterByColumnValue("bigs", "status", "NEW".getBytes());
    	ResultScanner rs = table.getScan(scan);
    	Result r = null;
    	while ( (r=rs.next())!=null) {
    		Pipeline ex = Pipeline.fromResultObject(r);
    		System.out.println("retrieved exploration "+ex.getPipelineNumber()+" "+ex.getStatusAsString());
    	}
    	
    }
    
	@Override
	public String getDescription() {
		return "command for testing stuff";
		
	}

}
