package HDFSWordC.HDFSWordC;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import HDFSWordC.HDFSWordC.TrendingTopicCount.IntSumReducer;
import HDFSWordC.HDFSWordC.TrendingTopicCount.TokenizerMapper;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class GetUserStatus {
	
	public static void main(String[] args) throws TwitterException {
		//String file1 = null;
				
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		//Prompt the user to enter the input and output path of the HDFS cluster
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter source path of HAdoop Cluster");
		String srcDirPath = sc.next();
		System.out.println("Enter destination path of HAdoop Cluster");
		String destDirPath = sc.next();
		
		
	
		
		

        try {
        	Configuration conf = new Configuration();
        	//To check if already the path of both source and destination dir exists
        	if(FileSystem.get(URI.create(srcDirPath.trim()),conf).exists(new Path(srcDirPath)))
        			{
        		 System.out.println("File already exists");
        			}
        	else if(FileSystem.get(URI.create(destDirPath.trim()),conf).exists(new Path(destDirPath)))
			{
		 System.out.println("DestinationFile already exists");
			}
        		
        	
        	else{
    		String[] startDate = {"2016-01-01","2016-01-06","2016-01-12","2016-01-18","2016-01-25"};
    		String[] endDate = {"2016-01-05","2016-01-11","2016-01-17","2016-01-24","2016-01-30"};		
    		
        	for(int i=0;i<5;i++){
        	Twitter twitter = TwitterFactory.getSingleton();
    		Query query = new Query("nyse");
    		query.setSince(startDate[i]);
    		query.setUntil(endDate[i]);
    		List<Status> statuses = twitter.search(query).getTweets();
    		    		        	
        	StringBuilder sb=new StringBuilder();
        	for(Status b: statuses) {
        		sb.append(b.getText());
      	}
        	String finalString=sb.toString();
        	InputStream stream = new ByteArrayInputStream(finalString.getBytes(StandardCharsets.UTF_8));
        	
    	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
    	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
    	    // Converting the dstPAth String into the uri(path) of hadoop
    	    
    	    FileSystem fs = FileSystem.get(URI.create(srcDirPath+"file"+i+".txt"), conf);
    	     OutputStream out = fs.create(new Path(srcDirPath+"file"+i+".txt"), new Progressable() {
    	      public void progress() {
    	        System.out.print(".");
    	      }
    	    
    	    });
    	    
    	    IOUtils.copyBytes(stream, out, 4096, true);
        	}
    	    //
    	    Configuration conf2 = new Configuration();
    	    conf2.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    	    conf2.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    	    conf2.set("mapreduce.framework.name", "yarn");
    	    Job job = Job.getInstance(conf2, "word count");
    	    job.setJarByClass(TrendingTopicCount.class);
    	    job.setMapperClass(TokenizerMapper.class);
    	    job.setCombinerClass(IntSumReducer.class);
    	    job.setReducerClass(IntSumReducer.class);
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(IntWritable.class);
    	    FileInputFormat.addInputPath(job, new Path(srcDirPath));
    	    FileOutputFormat.setOutputPath(job, new Path(destDirPath));
    	    System.exit(job.waitForCompletion(true) ? 0 : 1);
        }}
        catch(Exception e ){
        	System.out.println("Exception caught"+e);
        }
        
        
	}
	
}

