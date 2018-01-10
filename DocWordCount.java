//Name: Sai kalyan Yeturu
//Email: syeturu1@uncc.edu

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class DocWordCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( DocWordCount.class);

   public static void main( String[] args) throws  Exception {
      int docWordCountRes  = ToolRunner .run( new DocWordCount(), args);
      System .exit(docWordCountRes);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " docwordcount ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
    	 //Converting current line Text to String
         String line  = lineText.toString();
         
         Text currentWord  = new Text();
         
         //Retrieving file name
         String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
         
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            
            //Adding file name to the word name with delimiter as the key
            word = word.toLowerCase()+"#####"+fileName;
            currentWord  = new Text(word);
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
    	  
         int wordCount  = 0;
         
         //looping values obtained by mapper
         for ( IntWritable count  : counts) {
            wordCount  += count.get();
         }
         
         //Writing wordname#####filename as key and count as value
         context.write(word,  new IntWritable(wordCount));
      }
   }
}