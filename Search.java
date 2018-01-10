//Name: Sai kalyan Yeturu
//Email: syeturu1@uncc.edu

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;


public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
	   
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   //Getting third argument which is user input query
	   String user_query = args[2];
	   
	//Creating configuration object
	  Configuration config = getConf();
	  FileSystem FS = FileSystem.get(config);
	  
	//Setting value of user input as configuration attribute
	  config.set("user_query", user_query);   
	   
      Job job  = Job .getInstance(getConf(), " search ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job, args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,DoubleWritable> {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
         //Getting user input from configuration attribute
         String query = context.getConfiguration().get("user_query");
         String[] query_words = query.toLowerCase().split(" ");
        		 
         String wordName = line.split("#####")[0];
         String term = line.split("#####")[1];
         String fileName = term.split("\\t")[0];
         Double tf_idf = Double.parseDouble(term.split("\\t")[1]);
         
         //This loop tries to matches word with the words in the  user query
         for(String word : query_words){
        	 if(word.equals(wordName)){
        		 //Adding key as the file name and value as the respective TFIDF value
        		 context.write(new Text(fileName), new DoubleWritable(tf_idf));
        	 }
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable> tfidf_values,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  double score = 0.0;
    	  
    	  //This loop adds the tfidf values of each file
    	  for(DoubleWritable value : tfidf_values){
    		  score += value.get();
    	  }
    	  
    	  context.write(word, new DoubleWritable(score));
      }
   }
}