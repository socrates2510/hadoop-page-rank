package com.basesdedatos.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class HadoopPageRanking extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new HadoopPageRanking(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isCompleted = copyInputFile("pagerank/in/"+args[0], "pagerank/result/iter00");
        int iterations = 10;
        if (args.length > 1)
        {
        	iterations = Integer.parseInt(args[1]);
        }
        
        if (!isCompleted) return 1;

        String lastResultPath = null;

        for (int runs = 0; runs < iterations; runs++) {
            String inPath = "pagerank/result/iter" + nf.format(runs);
            lastResultPath = "pagerank/result/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

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

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(HadoopPageRanking.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        return rankCalculator.waitForCompletion(true);
    }

    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(HadoopPageRanking.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }

}
