import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/*As an example, here are the 3,000,000th and 3,000,001st lines from the
 a file of the English 1-grams (googlebooks-eng-all-1gram-20120701-a.gz):
circumvallate   1978   335    91
circumvallate   1979   261    91
The first line tells us that in 1978, the word "circumvallate"
(which means "surround with a rampart or other fortification", in case you were wondering)
occurred 335 times overall, in 91 distinct books of our sample.*/


public class StepOne {
    public static String[] stopWords = {"a",
            "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
            "alone", "along", "already", "also", "although", "always", "am", "among", "amongst",
            "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything",
            "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because",
            "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
            "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but",
            "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldn't",
            "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each",
            "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc",
            "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few",
            "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former",
            "formerly", "forty", "found", "four", "from", "front", "full", "further", "get",
            "give", "go", "had", "has", "hasn't", "have", "he", "hence", "her", "here", "hereafter",
            "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however",
            "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its",
            "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may",
            "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move",
            "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless",
            "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now",
            "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other",
            "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part",
            "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed",
            "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since",
            "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime",
            "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that",
            "the", "their", "them", "themselves", "then", "thence", "there", "thereafter",
            "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
            "third", "this", "those", "though", "three", "through", "throughout", "thru",
            "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty",
            "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well",
            "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter",
            "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
            "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
            "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};
    public static final Set<String> stopSet = new HashSet<>(Arrays.asList(stopWords));

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] line = val.toString().split("\t");
            String ngram = line[0]; //the word
            if (!stopSet.contains(ngram)) {
                String decade = line[1].substring(0, line[1].length() - 1);
                decade = new StringBuilder().append(decade).append("0").toString(); //  the decade. for example 1990
                String ngram_and_decade = ngram + "-" + decade; //for example apple-1990
                int occurs = Integer.parseInt(line[2]);

                Text ngram_and_decade_Key = new Text(), occurs_Value = new Text(), decade_counter_Key = new Text();
                occurs_Value.set(String.format("%d", occurs));
                decade_counter_Key.set(String.format("*^&decade_couneter %s", decade));
                ngram_and_decade_Key.set(String.format("%s", ngram_and_decade));
                context.write(ngram_and_decade_Key, occurs_Value); // this is how we will count apperance of each word per deceade
                // expamle apple-1990, 20
                context.write(decade_counter_Key, occurs_Value);
            } // this is how we will cound number of words per decade
            // example *^&decade_couneter 1990 , 20
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("key is\t" + key);
            String w1 = key.toString();
            Text newKey = new Text(), newValue = new Text();
            long totalOccurs = 0L;
            for (Text value : values) {
                totalOccurs += Long.parseLong(value.toString());
                System.out.println("value is \t" + value);
            }

            newKey.set(String.format("%s", w1));
            newValue.set(String.format("%d", totalOccurs));
            System.out.printf("final result is\n%s : %d\n\n", w1, totalOccurs);
            context.write(newKey, newValue);
            // expamle apple-1990, 20
            // expamle apple-1990, 30
            // --> apple-1990 , 50

            // example *^&decade_couneter 1990 , 20
            // example *^&decade_couneter 1990 , 230
            // --> 1990, 250
        }
    }

    private static class MyPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("starting EMR");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StepOne.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(StepOne.MyPartitioner.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //TODO change the input format
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        String output = args[1];
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
