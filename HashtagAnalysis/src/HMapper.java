import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HMapper extends Mapper<Object, Text, Text, IntWritable> { 
	
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String tweets = value.toString();
        
		//if there is a 4th semicolon...
    	if(StringUtils.ordinalIndexOf(tweets,";",4)>-1){

	        int startIndex = StringUtils.ordinalIndexOf(tweets,";",2) + 1;
	        
	        String tweet = tweets.substring(startIndex,tweets.lastIndexOf(';'));
	        tweet = tweet.toLowerCase();

			Matcher matcher = Pattern.compile("#go\\s*(\\w+)").matcher(tweet);
			
	        while(matcher.find()){
	        	try{
	        		
		        	String team = tweet.substring(matcher.start() + 3, matcher.start() + 7);
		        	if(team.equals("team")){
		        		
		        		context.write(new Text(tweet.substring(matcher.start() + 7, matcher.end())), one);
		        		break;
		        	}
		        	
	        	} catch(StringIndexOutOfBoundsException e){
	        		
	        	}
        		context.write(new Text(matcher.group(1)), one);
	        	
	        }
	        
	        matcher = Pattern.compile("#team\\*s*(\\w+)").matcher(tweet);
            
	        while(matcher.find()){
	        	context.write(new Text(matcher.group(1)), one);
	        	
	        }

    	}
    
    }
}
