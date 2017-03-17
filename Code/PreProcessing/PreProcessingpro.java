import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PreProcessingpro {

    	final static IntWritable totalLines = new IntWritable(0);

    	public static void main(String[] args) throws Exception {

		// Setting the configuration such that we can upload the documents in command line
        	Configuration conf = new Configuration();
        	conf.set("stopWords", args[2]);
        	conf.set("wordCount", args[3]);

        	Job job = Job.getInstance(conf, "PreProcessing");
        	job.setJarByClass(PreProcessingpro.class);
        	job.setMapperClass(Map.class);
        	job.setReducerClass(Reduce.class);

        	job.setOutputKeyClass(IntWritable.class);
        	job.setOutputValueClass(Text.class);

        	FileInputFormat.addInputPath(job, new Path(args[0]));
        	FileOutputFormat.setOutputPath(job, new Path(args[1]));

        	job.waitForCompletion(true);

		// Problem with the number of lines: does not seem to work…
		System.out.println("Number of lines = " + totalLines); 
	}

    	public static class Map extends Mapper<Object, Text, IntWritable, Text>{
		
		// Initialise the stop words 
        	private Text wordText = new Text();
        	private String word = new String();
        	String stopWords = "";

		// Upload the stop words
        	protected void setup(Context context) throws IOException, InterruptedException {

			// Go get the file
            		String stopWordsPath = context.getConfiguration().get("stopWords");
            		Path openStopWords = new Path(stopWordsPath);
            		FileSystem fs = FileSystem.get(new Configuration());
            		BufferedReader stopWordsFile = new BufferedReader(new InputStreamReader(fs.open(openStopWords)));
            		String line;
            		line= stopWordsFile.readLine();

			// Upload each word one by one 
            		try{
                		while (line != null){
                    			stopWords += line + ",";
                    			line = stopWordsFile.readLine();
                    		}
            		} finally {
              			stopWordsFile.close();
            		}
        	}

        	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Initialisation 
            		String wordsofline = value.toString();
            		String seenwords = "";
            		StringTokenizer token = new StringTokenizer(wordsofline , " %\t\n\r&\\-_!.;,()\'\"/:+=$[]§?#*|{}~<>@`");

			// If word has not been seen before in the line and is not a stop word, keep it
            		while (token.hasMoreTokens()) {
                		wordText.set(token.nextToken());
                		word = wordText.toString();
                		if (!stopWords.contains(word) && !seenwords.contains(word)){
                    		seenwords = seenwords + word + " ";
                		}
            		}
            		if (seenwords.length()>0) {
                		totalLines.set(totalLines.get()+1);
                		seenwords = seenwords.substring(0, seenwords.length()-1);
                		Text seenwordsText = new Text(seenwords);
                		context.write(totalLines, seenwordsText);
            		}
		}
    	}

    	public static class Reduce extends Reducer<IntWritable,Text,IntWritable,Text> {
        
	// Create a sort of dictionary to count the occurrences of words to order them
        private HashMap<String, Integer> dict = new HashMap<String, Integer>();

	// Import word count
        protected void setup(Context context) throws IOException, InterruptedException {
		
		// Load file
		String wordCountPath = context.getConfiguration().get("wordCount");
            	Path openWordCount = new Path(wordCountPath);
            	FileSystem fs = FileSystem.get(new Configuration());
            	BufferedReader wordCountFile =new BufferedReader(new InputStreamReader(fs.open(openWordCount)));
            	String line;
            	line = wordCountFile.readLine();

		// For each line, get the word and the number of values
            	try{
                while (line != null){
                    String keyvalue[] = line.split("\t");
                    String word = keyvalue[0];
                    int nbcount = Integer.parseInt(keyvalue[1]);
                    dict.put(word, nbcount);
                    line = wordCountFile.readLine();
                }
            	} finally {
              		wordCountFile.close();
            	}
        }

        public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

            for (Text value : values){

		// Create a sort of sub dictionary per line associating word - count
                HashMap<String, Integer> subdict = new HashMap<String, Integer>();

                // Take the words and counts of the associated line
                String line = value.toString();
                StringTokenizer token = new StringTokenizer(line);

                while (token.hasMoreTokens()) {
                    String word = token.nextToken();
                    subdict.put(word.toLowerCase(), dict.get(word));
                }

                // Order the words of that line
                List<String> HMKeys = new ArrayList<String>(subdict.keySet());
                List<Integer> HMValues = new ArrayList<Integer>(subdict.values());
                Collections.sort(HMValues);

		// Use that ordered list
                LinkedList<String> list = new LinkedList<String>();
                Iterator<Integer> HMItvalue = HMValues.iterator();

                while (HMItvalue.hasNext()) {

                    Integer v = HMItvalue.next();
                    Iterator<String> HMItkey = HMKeys.iterator();

                    while (HMItkey.hasNext()) {

                        String sortedWords = HMItkey.next();
                        Integer comp1 = subdict.get(sortedWords);
                        Integer comp2 = v;

                        if (comp1.equals(comp2)) {
                            HMItkey.remove();
                            list.add(sortedWords);
                        }

                    }
                }
		
		// Join sorted lists of words
                String sortedString  = String.join(" ", list);
                Text result = new Text(sortedString); 
                context.write(key, result);
            }
        }
    }
}
