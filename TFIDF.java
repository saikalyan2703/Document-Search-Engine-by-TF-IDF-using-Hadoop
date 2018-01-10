//Name: Sai kalyan Yeturu
//Email: syeturu1@uncc.edu

import java.io.Console;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.fs.FileSystem;
import com.google.common.collect.Iterators;


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
	   
	   String[] intermediate_args = {args[0],"IntermediateOutput"};
	  //Running TermFrequency job
	  int termFrequencyRes  = ToolRunner .run( new TermFrequency(), intermediate_args);
	  
	  //Running TFIDF job
      int tfidfRes  = ToolRunner .run( new TFIDF(), args);
      
      System .exit(termFrequencyRes);
      System .exit(tfidfRes);
   }

   public int run( String[] args) throws  Exception {
	   
	  //Creating configuration object 
	  Configuration config = getConf();
	  FileSystem file_system = FileSystem.get(config);
	  
	  //Setting value of total files as configuration attribute
	  final int total_files = file_system.listStatus(new Path(args[0])).length;
	  config.setInt("total_files", total_files);   
	   
      Job job  = Job .getInstance(getConf(), " tfidf ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job, "IntermediateOutput");
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,Text > {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
         
         Text keyWord=new Text();
         Text valueWord=new Text();
         
         //Splitting line with tab space between key and value in the text file generate by TermFrequency reducer
         String[] x = line.split("\\t");
         
         //Splitting with delimiter between word name and file name
         String wordName = line.split("#####")[0];
         String term = line.split("#####")[1];
         String fileName = term.split("\\t")[0];
         
         //Keeping value as the <filename=term frequency>
         String value = fileName + "=" + term.split("\\t")[1];
         
         keyWord  = new Text(wordName);
         valueWord  = new Text(value);
         context.write(keyWord,valueWord);
      }
   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text> valueWords,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  //retrieving value of total number of files from configuration attribute
    	  int total_files = context.getConfiguration().getInt("total_files", 0);	  
    	
    	  double tf_idf,idf;
    	  int req_files = 0;
    	  
    	  List<String> fileName = new ArrayList<String>();
    	  List<Double> wordFrequency = new ArrayList<Double>();
    	  
    	  //loop to store file names and word names in the array lists
    	  for(Text value : valueWords){
    		  String[] x = value.toString().split("=");
    		  fileName.add(x[0]);
    		  wordFrequency.add(Double.parseDouble(x[1]));
    		  req_files = req_files + 1; 
    	  }
    	  
    	  //IDF is calculated
    	  idf = Math.log10(1+(total_files/fileName.size()));
    	  
    	  //TFIDF is calculated for each word
    	  for(int i = 0;i<fileName.size();i++){
    		  tf_idf = idf * wordFrequency.get(i);
    		  context.write(new Text(word.toString()+"#####"+fileName.get(i)),new DoubleWritable(tf_idf));
    	  }
    	     	  
      }
   }
}