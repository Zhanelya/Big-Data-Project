import inintpair.IntIntPair;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TAvgReducer extends Reducer<Text, IntIntPair, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntIntPair> values, Context context)

              throws IOException, InterruptedException {

        int sum_length = 0;
        int sum_tweets = 0;

        for (IntIntPair value : values) {

        	sum_length = sum_length + Integer.parseInt(value.getFirst().toString()); 
        	sum_tweets = sum_tweets + Integer.parseInt(value.getSecond().toString()); 

        }

               result.set(sum_length/sum_tweets);

        context.write(key, result);

    }

}