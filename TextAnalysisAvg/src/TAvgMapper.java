import java.io.IOException;
import inintpair.IntIntPair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

public class TAvgMapper extends Mapper<Object, Text, Text, IntIntPair> { 
    private Text emit_key = new Text("avg");
    private IntIntPair data = new IntIntPair();
    private final IntWritable one = new IntWritable(1);
    
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    // Format per tweet is id;date;hashtags;tweet;
    		String tweets = value.toString();
		    if(StringUtils.ordinalIndexOf(tweets,";",4)>-1){
		        int startIndex = StringUtils.ordinalIndexOf(tweets,";",3) + 1;
		        String tweet = tweets.substring(startIndex,tweets.lastIndexOf(';'));
		        tweet = tweet.replaceAll("[^a-zA-Z0-9]+", "1");
		        data.set(new IntWritable(tweet.length()), one);
	        	context.write(emit_key, data);
		      }
	}
        
}    



