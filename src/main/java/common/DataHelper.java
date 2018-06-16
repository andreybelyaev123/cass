package common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataHelper {
	private DataHelper() {}
	
	public static List<Map<String,String>> readFromFile(String fileName) throws FileNotFoundException, IOException {
		List<Map<String,String>> data = new ArrayList<>();
		try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    for(String line; (line = br.readLine()) != null; ) {
		        String [] values = line.split(",");
		        if (values.length == 0)
		        	continue;
		        Map<String,String> lineData = new HashMap<>();
		        for (String value : values) {
		        	if (value == null || value.isEmpty())
		        		continue;
		        	int pos = value.indexOf(':');
		        	if (pos != -1) {
		        		lineData.put(value.substring(0,pos), value.substring(pos + 1));
		        	}
		        }
		        if (!lineData.isEmpty())
		        	data.add(lineData);
		    }
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		return data;
	}
}
