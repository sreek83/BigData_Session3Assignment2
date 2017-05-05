package test.hadoop.practice;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.*;

public class HadoopSession3_Assign2_Task5 {

	public static void main(String[] args) throws IOException,URISyntaxException {
		
	System.out.println("HadoopAssignment7 --> Start");
	Configuration conf = new Configuration();
	
	FileSystem fs =FileSystem.get(new URI("hdfs://localhost:9000"),conf);
	Path fileName = new Path(args[0]);
	System.out.println("HadoopAssignment7 --> file :"+args[0]);
	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(fileName)));
    String line;
    line=br.readLine();
     
    while (line != null){
             System.out.println(line);
             line=br.readLine();
    }
    System.out.println("HadoopAssignment7 --> End");
	
	}
	
}
