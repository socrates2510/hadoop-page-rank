package com.basesdedatos.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


/**
 * @author csanchez, cgamboa, jcbrenes
 *
 */
public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int pageTabIndex = value.find("\t");
		int rankTabIndex = value.find("\t", pageTabIndex+1);
		String page;
		String pageWithRank;
		if (pageTabIndex==-1)
		{
			pageTabIndex=value.find(" ");
			rankTabIndex = pageTabIndex;
			if (pageTabIndex==-1)
			{
				page = Text.decode(value.getBytes(), 0, value.getLength());
			}
			else
			{
				page = Text.decode(value.getBytes(), 0, pageTabIndex);
			}
			pageWithRank = page+"\t1.0\t";
		}
		else
		{
			page = Text.decode(value.getBytes(), 0, pageTabIndex);
			pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
		}
		// Mark page as an Existing page (ignore red wiki-links)
		context.write(new Text(page), new Text("!"));
		// Skip pages with no links.
		if(rankTabIndex == -1) return;
		String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
		links=links.replace(" ", ",");
		String[] allOtherPages = links.split(",");
		int totalLinks = allOtherPages.length;
		for (String otherPage : allOtherPages){
			Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
			context.write(new Text(otherPage), pageRankTotalLinks);
		}
		// Put the original links of the page for the reduce output
		context.write(new Text(page), new Text("|" + links));
	}	 
}