package inintpair;
import java.io.*;
import org.apache.hadoop.io.*;

public class IntIntPair implements WritableComparable<IntIntPair> {

   private IntWritable first;
   private IntWritable second;

   public IntIntPair() {
      set(new IntWritable(), new IntWritable());
   }

   public IntIntPair(int first, int second) {
      set(new IntWritable(first), new IntWritable(second));
   }

   public void set(IntWritable first, IntWritable second) {
      this.first = first;
      this.second = second;
   }

   public IntWritable getFirst() {
      return first;
   }

   public IntWritable getSecond() {
      return second;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      first.write(out);
      second.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      first.readFields(in);
      second.readFields(in);
   }

   @Override
   public int hashCode() {
      return first.hashCode() * 163 + second.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof IntIntPair) {
         IntIntPair tp = (IntIntPair) o;
         return first.equals(tp.first) && second.equals(tp.second);
      }
      return false;
   }

   @Override
   public String toString() {
      return first + "\t" + second;
   }

   @Override
   public int compareTo(IntIntPair tp) {
      int cmp = first.compareTo(tp.first);
      if (cmp != 0) {
         return cmp;
      }
      return second.compareTo(tp.second);
   }
}