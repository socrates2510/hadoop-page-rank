package com.basesdedatos.hadoop;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank extends Configured implements Tool {
    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
    }

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public static final Log LOG = LogFactory.getLog(Reduce.class);

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int pageTabIndex = value.find("\t");
		int rankTabIndex = value.find("\t", pageTabIndex+1);
		
		// Skip pages with no links.
		if(rankTabIndex == -1) return;
		
		String page;
		String pageWithRank;
		
		page = Text.decode(value.getBytes(), 0, pageTabIndex); // <name of the node>
		pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1); // <name of the node>\t<pagerank>\t
		
		String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1)); // <links>,<links>,<links>
		links=links.replace(" ", ",");
		String[] outLinkPages = links.split(",");
		int totalLinks = outLinkPages.length;
		for (String otherPage : outLinkPages){
			Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
			LOG.info("MAP:" + otherPage + "\t" + pageRankTotalLinks.toString());
			output.collect(new Text(otherPage), pageRankTotalLinks); // [<outlink> , <page node>\t<pagerank>\t<total outlinks>]
		}
		// Put the original links of the page for the reduce output
		output.collect(new Text(page), new Text("|" + links)); // [<page node> , <outlinks>]
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public static final Log LOG = LogFactory.getLog(Reduce.class);
	private static final float damping = 0.85F;
	
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
      String[] split;
      float sumShareOtherPageRanks = 0;
      String links = "";
      String pageWithRank;

      // For each otherPage: 
      // - check control characters
      // - calculate pageRank share <rank> / count(<links>)
      // - add the share to sumShareOtherPageRanks
      Text value;
      while (values.hasNext()){
    	  value = values.next();
    	  pageWithRank = value.toString();

          if(pageWithRank.startsWith("|")){
              links = "\t"+pageWithRank.substring(1); // Reconstruct the graph
              continue;
          }

          split = pageWithRank.split("\\t"); // Split 
          
          float pageRank = Float.valueOf(split[1]);
          int countOutLinks = Integer.valueOf(split[2]);
          
          sumShareOtherPageRanks += (pageRank/countOutLinks);
      }

      float newRank = damping * sumShareOtherPageRanks + (1-damping);
      
      output.collect(key, new Text(newRank + links));

    }
  }
  
  public static class RankingMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {


		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<FloatWritable, Text> output, Reporter reporter)
				throws IOException {
	        String[] pageAndRank = getPageAndRank(key, value);
	        
	        float parseFloat = Float.parseFloat(pageAndRank[1]);
	        
	        Text page = new Text(pageAndRank[0]);
	        FloatWritable rank = new FloatWritable(parseFloat);

	        output.collect(rank, page);
			
		}
	    
	    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
	        String[] pageAndRank = new String[2];
	        int tabPageIndex = value.find("\t");
	        int tabRankIndex = value.find("\t", tabPageIndex + 1);
	        
	        // no tab after rank (when there are no links)
	        int end;
	        if (tabRankIndex == -1) {
	            end = value.getLength() - (tabPageIndex + 1);
	        } else {
	            end = tabRankIndex - (tabPageIndex + 1);
	        }
	        
	        pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
	        pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);
	        
	        return pageAndRank;
	    }
	    
	}

  public boolean runRankCalculation(String inputPath, String outputPath, int maps, int reduces) throws Exception {
	    JobConf conf = new JobConf(PageRank.class);
	    conf.setJobName("pagerank");

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);

	    conf.setMapperClass(Map.class);
	    conf.setReducerClass(Reduce.class);

	    conf.setNumMapTasks(maps);
	    conf.setNumReduceTasks(reduces);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(inputPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

	    RunningJob job = JobClient.runJob(conf);

        job.waitForCompletion();
        
        return job.isSuccessful();
  }

  private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

	    JobConf conf = new JobConf(PageRank.class);
	    conf.setJobName("rankOrdering");

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(FloatWritable.class);

	    conf.setMapperClass(RankingMapper.class);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(inputPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

	    RunningJob job = JobClient.runJob(conf);

      job.waitForCompletion();
      
      return job.isSuccessful();
  }
	
	@Override
	public int run(String[] args) throws Exception {
        boolean isCompleted = copyInputFile("pagerank/in/"+args[0], "pagerank/result/iter00");
        int iterations = 10;
        int maps = 3;
        int reduces = 2;
        if (args.length > 1)
        {
        	iterations = Integer.parseInt(args[1]);
        }
        
        if (args.length > 2)
        {
        	maps = Integer.parseInt(args[2]);
        }
        
        if (args.length > 3)
        {
        	reduces = Integer.parseInt(args[3]);
        }
        
        if (!isCompleted) return 1;

        String lastResultPath = null;

        for (int runs = 0; runs < iterations; runs++) {
            String inPath = "pagerank/result/iter" + nf.format(runs);
            lastResultPath = "pagerank/result/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath, maps, reduces);

            if (!isCompleted) return 1;
        }

        isCompleted = runRankOrdering(lastResultPath, "pagerank/result/final");

        if (!isCompleted) return 1;
        return 0;
	}
    
    
    public boolean copyInputFile(String inputFile, String outputFile) throws IOException
    {
    	Configuration config = new Configuration();
    	FileSystem hdfs = FileSystem.get(config);
    	Path inputPath = new Path(inputFile);
    	Path outputPath = new Path(outputFile);
    		
    	boolean success = FileUtil.copy(hdfs, inputPath, hdfs, outputPath, false, true, config);    	
    	return success;
    }
}