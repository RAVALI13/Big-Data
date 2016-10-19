package HDFSFileUpload.HDFSFileUpload;



	import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Multiset.Entry;

	public class CoOccurence extends Configured implements Tool {

	  private static final Logger LOG = Logger.getLogger(CoOccurence.class);

	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new CoOccurence(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception {
	    Job job = Job.getInstance(getConf(), "wordcount");
	    for (int i = 0; i < args.length; i += 1) {
	      if ("-skip".equals(args[i])) {
	        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
	        i += 1;
	        job.addCacheFile(new Path(args[i]).toUri());
	        // this demonstrates logging
	        LOG.info("Added file to the distributed cache: " + args[i]);
	      }
	    }
	    job.setJarByClass(this.getClass());
	    // Use TextInputFormat, the default unless job.setInputFormatClass is used
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
		job.addCacheFile(new Path(args[3]).toUri());
	    job.setMapperClass(Map.class);
	    job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(MapWritable.class);
//	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	    job.getConfiguration().set("Length", args[2]);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private MapWritable occurrenceMap = new MapWritable();
	    private Text word = new Text();
	    private boolean caseSensitive = false;
	    private long numRecords = 0;
	    private String input;
	    private Set<String> patternsToSkip = new HashSet<String>();
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	    protected void setup(Mapper.Context context)
	        throws IOException,
	        InterruptedException {
	      if (context.getInputSplit() instanceof FileSplit) {
	        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
	      } else {
	        this.input = context.getInputSplit().toString();
	      }
	      Configuration config = context.getConfiguration();
	      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
	      if (config.getBoolean("wordcount.skip.patterns", false)) {
	        URI[] localPaths = context.getCacheFiles();
	        parseSkipFile(localPaths[0]);
	      }
	    }

	    private void parseSkipFile(URI patternsURI) {
	      LOG.info("Added file to the distributed cache: " + patternsURI);
	      try {
	        BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
	        String pattern;
	        while ((pattern = fis.readLine()) != null) {
	          patternsToSkip.add(pattern);
	        }
	      } catch (IOException ioe) {
	        System.err.println("Caught exception while parsing the cached file '"
	            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
	      }
	    }

	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	    	int wordLength=Integer.parseInt(context.getConfiguration().get("Length"));
	    	List<String> tokensarr=new ArrayList<String>();
	      String line = lineText.toString();
	      if (!caseSensitive) {
	        line = line.toLowerCase();
	      }
	      Text currentWord = new Text();
	      for (String word : WORD_BOUNDARY.split(line)) {
	    	  if(word.length()==5){
	        if (word.isEmpty() || patternsToSkip.contains(word)) {
	            continue;
	        }
	        tokensarr.add(word);
	    	  }
	      }
	      int neighbors=2;
	      String[] tokens=new String[tokensarr.size()];
	      for (int i=0; i<tokensarr.size();i++) {
	    	  tokens[i]= tokensarr.get(i);
		}
	        if (tokens.length > 1) {
	            for (int i = 0; i < tokens.length; i++) {
	                word.set(tokens[i]);
	                occurrenceMap.clear();

	                int start = (i - neighbors < 0) ? 0 : i - neighbors;
	                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	                 for (int j = start; j <= end; j++) {
	                      if (j == i) continue;
	                      Text neighbor = new Text(tokens[j]);
	                      if(occurrenceMap.containsKey(neighbor)){
	                         IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
	                         count.set(count.get()+1);
	                      }else{
	                         occurrenceMap.put(neighbor,new IntWritable(1));
	                      }
	                 }
	                context.write(word,occurrenceMap);
	           }
	        //currentWord = new Text(word);
	          //  context.write(currentWord,one);
	        
	            
	        }             
	      }
	    }

	  public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
		  private MapWritable incrementingMap = new MapWritable();
		  @Override
		    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		        incrementingMap.clear();
		        for (MapWritable value : values) {
		            addAll(value);
		            
		        }
		        StringBuilder sb= new StringBuilder("[");
		        for (java.util.Map.Entry<Writable, Writable> entry : incrementingMap.entrySet()){
		        	if(sb.toString().length()!=1)
		        	{
		        		sb.append(",");
		        	}
		        	sb.append(entry.getKey()+":"+entry.getValue());
		        	}
		        sb.append(",");
		        context.write(key, new Text(sb.toString()));
		    }

		    private void addAll(MapWritable mapWritable) {
		        Set<Writable> keys = mapWritable.keySet();
		        for (Writable key : keys) {
		            IntWritable fromCount = (IntWritable) mapWritable.get(key);
		            if (incrementingMap.containsKey(key)) {
		                IntWritable count = (IntWritable) incrementingMap.get(key);
		                count.set(count.get() + fromCount.get());
		            } else {
		                incrementingMap.put(key, fromCount);
		            }
		        }
		    }
	  }
	}



