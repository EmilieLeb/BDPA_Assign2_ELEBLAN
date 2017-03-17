import java.net.URI;
import java.lang.Math;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class PairWisepro {

	// Set a counter to have the number of comparisons computed
	static enum counting {NUMBER_OF_COMPARISONS};

	public static class Map extends Mapper<Object, Text, Text, Text>{

		// Since we did not manage to find the exact line number, we set it arbitrarily (50, then 1000 and more if we can)
        	private int fullSize = 1000;

		// Create another kind of counter to be sure we don t exceed the fullSize
        	private IntWritable lineKey = new IntWritable(0);

        	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        		lineKey.set(lineKey.get()+1);
            		
			// For each line as long as we are under fullSize limit… 
			if (lineKey.get() <= fullSize){

				// We split the line between the line number and the string
                		String [] lineValueIt = value.toString().split("\t");
                		String lineValue = lineValueIt[0];
                		int lineKey = Integer.parseInt(lineValueIt[0]);
                		String line = lineValueIt[1];
                		Text lineText = new Text(line);
                
                		// We then create all pairs of lines that we will use for the similarity
                		for (int compLineKey = 1; compLineKey<=fullSize; compLineKey++){
                    			String newKey = new String();
                    			String compLineValue = String.valueOf(compLineKey);
					// According to their positions we put the first one first in the comparison couple
                    			if (compLineKey != lineKey) {
                        			if (lineKey < compLineKey){
                            				newKey = lineValue + "," + compLineValue;
                        			} else {
                            				newKey = compLineValue + "," + lineValue;
                        			}
                        			Text newKeyText = new Text(newKey);
                        			context.write(newKeyText, lineText);
                    			}
                		}
            		}
        	}
    	}


    	public static class Reduce extends Reducer<Text,Text,Text,Text> {

        	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            		// We then create an array with the lines of the same key
            		List<String> linesOfKey = new ArrayList<String>(2);
			for (Text value : values){
                		linesOfKey.add(value.toString());
            		}

            		// After checking that we compute a couple only once . . 
            		if (linesOfKey.size() == 2){
                		String d1 = linesOfKey.get(0);
                		String d2 = linesOfKey.get(1);
                		String [] wordsOf1 = d1.split(" ");
                		String [] wordsOf2 = d2.split(" ");

                		// .. We compute the Jacquard similarity as enunciated in the assignment
                		int intersection = 0;
                		int union = 0;

				// inspiration from : https://github.com/tdebatty/java-string-similarity/blob/master/src/main/java/info/debatty/java/stringsimilarity/Jaccard.java  
				for (String word : wordsOf1){

                			// If a word is in both, add one to intersection. Always add one to union.
                    			if (d2.contains(word)){
                        			intersection ++;
                   	 		} 
                        		union++;
					// Now check the leftover part: only add to union if we have not seen the word before, i.e. if it is only in wordsOf2
                			for (String word : wordsOf2){
                    				if (!(d1.contains(word))) {
                        				union++;
                				}

                		float Jacsim = (float) intersection / union;

                		// We now only return the couples that have a Jacquard similarity over 0.8
                		if (Jacsim > 0.8){
                    			String out = d1 + " - " + d2 + " - Jacsim = " + String.valueOf(Jacsim);
                    			Text value = new Text(out); 
                    			context.write(key, value);
                		}
				// Increment our counter !!
                		context.getCounter(counting.NUMBER_OF_COMPARISONS).increment(1);
            		}
        	}
    	}

    	public static void main(String[] args) throws Exception {
        	// As usual..
		Configuration conf = new Configuration();
        	Job job = Job.getInstance(conf, "PairWise");
        	job.setJarByClass(PairWisepro.class);
        	job.setMapperClass(Map.class);
        	job.setReducerClass(Reduce.class);
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(Text.class);
        	FileInputFormat.addInputPath(job, new Path(args[0]));
        	FileOutputFormat.setOutputPath(job, new Path(args[1]));

        	job.waitForCompletion(true);
    	}
}
