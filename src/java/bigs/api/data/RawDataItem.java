package bigs.api.data;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import net.sf.jmimemagic.Magic;
import bigs.api.core.BIGSException;
import bigs.api.utils.TextUtils;
import bigs.core.utils.Data;
public class RawDataItem extends DataItem {

	byte[] bytes;
	
	Map<String, String> metadata = new HashMap<String, String>();
	
	public byte[] getBytes() {
		return this.bytes;
	}

	public Map<String, String> getMetadata() {
		return this.metadata;
	}
	
	public void setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
	}
	
	public void setBytes(byte[] data) {
		this.bytes = data;
	}
	
	@Override
	public String toTextRepresentation() {
		return new String(bytes);
	}

	@Override
	public void fromTextRepresentation(String textRepresentation) {
		bytes = textRepresentation.getBytes();
	}
	
	public void setBytesAndRowkeyFromFile(File file) {
		if (!file.exists()) {
			throw new BIGSException("file "+file.getAbsolutePath()+" does not exist");
		}
		try {
			this.bytes = Data.getBytesFromFile(file);
			this.setRowkey(file.getName());
			this.metadata.put("path", file.getAbsolutePath());
			this.metadata.put("size", new Long(bytes.length).toString());
			this.metadata.put("importDate", TextUtils.FULLDATE.format(Calendar.getInstance().getTime()));
			String mimeType = "unknown";
			try {
				mimeType = Magic.getMagicMatch(bytes).getMimeType().toString();
			} catch (Exception e) {
				throw new BIGSException("");
			} catch (java.lang.NoClassDefFoundError ne) {
				// do nothing, we assume the library does not know the file
			}
			this.metadata.put("mimetype", mimeType);
			
		} catch (IOException e) {
			throw new BIGSException("could not open file "+file.getName()+", "+e.getMessage());
		}
	}


}
