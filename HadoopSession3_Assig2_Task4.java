package test.hadoop.practice;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;


public class HadoopSession3_Assig2_Task4{

	public static void main(String[] args) throws IOException, URISyntaxException{
		System.out.println("HadoopAssignment5 --> Start");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( new URI("hdfs://localhost:9000"),conf);
		int strt_ts = 0;
		long end_ts = 99999999999999L;
		if(args.length>0) {
			
			strt_ts = Integer.parseInt(args[0]);
			end_ts = Long.parseLong(args[1]);
			
		}
		
		Path dirPath = new Path("/user/srikanth/hadoop");
		displayfiles(fs,dirPath,strt_ts,end_ts);
		System.out.println("HadoopAssignment5 --> End");

	}
	
	static void  displayfiles(FileSystem fs,Path dirPath, long strt_ts,long end_ts) {
		
		try{
			System.out.println("HadoopAssignment5 --> displayfiles -->Start");

			
			long modTime = 0;
		
			FileStatus []filesStatusArry = fs.listStatus(dirPath);
			for(FileStatus filestatus : filesStatusArry) {
				if(filestatus.isDirectory()){
					displayfiles(fs,filestatus.getPath(),0L,99999999999999999L);
															
				}else if(filestatus.isFile()) {
					
					modTime = filestatus.getModificationTime();
					System.out.println("File : "+modTime);
					if(modTime>strt_ts && modTime <=end_ts)
						System.out.println("File : "+filestatus.getPath()  +"-"+modTime);
				}
				
			}
		}catch(IOException e) {
			
		}
		
		
		
	}

}
