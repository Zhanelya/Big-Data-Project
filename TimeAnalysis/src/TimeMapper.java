import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.http.ParseException;
import org.apache.commons.lang.StringUtils;

public class TimeMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private Text data = new Text();
    private final IntWritable one = new IntWritable(1);
    
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    // Format per tweet is id;date;hashtags;tweet;
    		String tweets = value.toString();
		    if(StringUtils.ordinalIndexOf(tweets,";",4)>-1){
		        int startIndex = StringUtils.ordinalIndexOf(tweets,";",1) + 1;
		        int finishIndex = StringUtils.ordinalIndexOf(tweets, ";", 2);
		        //split by ',' and take the first element (2012-07-27, 20:48:57, BST)
			    String tweet_date = tweets.substring(startIndex,finishIndex).split(", ")[0];
		        data.set(tweet_date);
	        	context.write(data, one);
		      }
	}
        
}    



