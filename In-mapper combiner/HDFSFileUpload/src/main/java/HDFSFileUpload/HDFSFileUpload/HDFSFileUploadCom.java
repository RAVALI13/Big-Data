package HDFSFileUpload.HDFSFileUpload;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import HDFSFileUpload.HDFSFileUpload.WordCountBooks.IntSumReducer;
import HDFSFileUpload.HDFSFileUpload.WordCountBooks.TokenizerMapper;

public class HDFSFileUploadCom {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		String[] fileUri = {"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
							"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
							"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
							"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
							"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
							"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"};
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter destination path of HAdoop Cluster");
		String destDirPath = sc.next();
		for (String inputFileURI : fileUri) {
			String destURI = destDirPath + inputFileURI.substring(inputFileURI.lastIndexOf('/'), inputFileURI.length());
			copyFile(inputFileURI,destURI);
			decompressFile(destURI);
			
		}
		
		
	}
		
	 
	static void copyFile(String fileUri, String dstPath) throws IOException
	{
		//InputStream in = new BufferedInputStream(new FileInputStream(srcPath));
		InputStream in = new BufferedInputStream(new URL(fileUri).openStream());
	    
	    Configuration conf = new Configuration();
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	    // Converting the dstPAth String into the uri(path) of hadoop
	    
	    FileSystem fs = FileSystem.get(URI.create(dstPath), conf);
	     OutputStream out = fs.create(new Path(dstPath), new Progressable() {
	      public void progress() {
	        System.out.print(".");
	      }
	    
	    });
	    
	    IOUtils.copyBytes(in, out, 4096, true);
	}
	
	static void decompressFile(String uri) throws IOException //uri is hdfsFilePAth of Archive
, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		
	    //Converted the string uri into actual uri.
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + uri);
	      System.exit(1);
	    }

	    String outputUri =
	      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

	    InputStream in = null;
	    OutputStream out = null;
	    try {
	      in = codec.createInputStream(fs.open(inputPath));
	      out = fs.create(new Path(outputUri));
	      IOUtils.copyBytes(in, out, conf);
	      
	    }
	    finally {
		      IOUtils.closeStream(in);
		      IOUtils.closeStream(out);
			    fs.delete(inputPath, true);
		    }
	 
	    
	  }
	}




