package test.hadoop.practice;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.*;
import org.apache.hadoop.fs.Path;

public class HadoopSession3_Assign2_Task6 {

	public static void main(String[] args) throws IOException,URISyntaxException {
		
		Configuration conf = new Configuration();
		FileSystem fs =FileSystem.get( new URI("hdfs://localhost:9000"),conf);
		copyFilefromLocal(fs,args[0],args[1]);
		

	}
	
	
	
	public static void copyFilefromLocal(FileSystem fs,String source_file,String dest_file)throws IOException {
		
		fs.copyFromLocalFile(new Path(source_file),new Path("hdfs://localhost:9000"+dest_file));
	}

}
