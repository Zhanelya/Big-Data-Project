import java.io.IOException;
import java.math.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

public class TMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private Text upperbound = new Text("0");
    private final IntWritable one = new IntWritable(1);
    
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    // Format per tweet is id;date;hashtags;tweet;
    		String tweets = value.toString();
		    if(StringUtils.ordinalIndexOf(tweets,";",4)>-1){
		        int startIndex = StringUtils.ordinalIndexOf(tweets,";",3) + 1;
		        String tweet = tweets.substring(startIndex,tweets.lastIndexOf(';'));
		        tweet = tweet.replaceAll("[^a-zA-Z0-9]+", "1");
		        if (tweet.length()<=140){
		        	int upperb = (int) (Math.ceil((float)tweet.length()/5)*5);
		        	int lowerb = (int) ((Math.ceil((float)tweet.length()/5)-1)*5 + 1);
					upperbound.set(String.valueOf(lowerb) + "-"+ String.valueOf(upperb));
		        }else{
		        	upperbound.set(">140");
		        }
		    	context.write(upperbound, one);
		        
		      }
	}
        
}    



