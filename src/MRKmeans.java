
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.net.URI;
import java.util.Random;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author Katsadas Ioannis 
 * @author Michailidou Anna Valentini 
 */
public class MRKmeans {

    public static Path pathCenters;//Path to centers.txt file
    private static int dimensions, numOfCenters;//Number of vector dimensions,number of centers

    /**
     * A Mapper class made for the first job of the assignment. Extends from the
     * class Mapper that is in hadoop libraries.On the setup method it read the
     * file that is on distributed cache and creates a set of stop words. As a
     * result it groups the words by the file they originated from.
     */
    public static class FirstPartMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable doc;
        private Set<String> stopWords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                URI[] uriPath = DistributedCache.getCacheFiles(context.getConfiguration());
                Path[] stopWordsFilePath = new Path[uriPath.length];
                for (int i = 0; i < uriPath.length; i++) {
                    stopWordsFilePath[i] = new Path(uriPath[i].getPath());
                }
                if (stopWordsFilePath != null && stopWordsFilePath.length > 0) {
                    for (Path stopWordFile : stopWordsFilePath) {
                        readFile(stopWordFile);
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in First Part in Mapper in method setup(): " + ex.getMessage());
            }
        }

        private void readFile(Path path) {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String lineWord = null;
                while ((lineWord = bufferedReader.readLine()) != null) {
                    stopWords.add(lineWord.toLowerCase());
                }
                bufferedReader.close();
            } catch (IOException ex) {
                System.err.println("Exception while reading stopwords file in First Part in method readFile(): " + ex.getMessage());
            }
        }

        /**
         * The method splits the input text that it gets into words and excludes
         * the punctuation and the stop words. then it adds to context each word
         * and the name of the file it originated from.
         *
         * @param key key input, Object
         * @param value value input, Text
         * @param context the result of the method(K,V)
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> okWords = new ArrayList<String>();
            //Read and split the line and exclude punctuation
            String[] items = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
            //exclude stopwords
            for (int i = 0; i < items.length; i++) {
                if (!stopWords.contains(items[i])) {
                    okWords.add(items[i]);
                }
            }
            items = okWords.toArray(new String[0]);
            //Stemming
            Stemmer s;
            for (int i = 0; i < items.length; i++) {
                s = new Stemmer();
                char word[] = items[i].toCharArray();
                s.add(word, word.length);
                s.stem();
                items[i] = s.toString();
            }
            //The rest for the Inverted Index

            //Check if items isn't empty
            if (items == null) {
                return;
            }
            //Put them to context
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            int index = Integer.parseInt(fileName.replace(".txt", ""));
            doc = new IntWritable(index);
            for (int i = 0; i < items.length; i++) {
                word.set(items[i]);
                context.write(this.word, this.doc);
            }
        }
    }

    /**
     * A Reducer class made for the second job of the assignment. Extends from
     * the class Reducer that is in hadoop libraries. It creates a vector for
     * each word with as many dimensions as the number of the files are.
     */
    public static class FirstPartReducer extends Reducer<Text, IntWritable, Text, IntArrayWritable> {

        /**
         * This method is called once for each cluster. For each word, it
         * iterates the name of the file that they came from and creates an
         * array as their position according to them (values),giving as an
         * output the cluster word and its position(array).
         *
         * @param clusterNum key input, IntWritable
         * @param values value input, WordInfoWritable
         * @param context the result of the method(K,V)
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //consturct the final array
            IntWritable[] temp = new IntWritable[dimensions];
            for (int k = 0; k < temp.length; k++) {
                temp[k] = new IntWritable(0);
            }
            for (IntWritable val : values) {
                temp[val.get() - 1] = new IntWritable(1);
            }
            IntArrayWritable result = new IntArrayWritable(temp);
            context.write(key, result);

        }
    }

    /**
     * A Mapper class made for the second job of the assignment. Extends from
     * the class Mapper that is in hadoop libraries.It distinguishes from each
     * file the words and their position and send them to the reducer along with
     * the cluster they are closer to.
     */
    public static class job2Mapper
            extends Mapper<Object, Text, IntWritable, WordInfoWritable> {

        private WordInfoWritable wordInfo = new WordInfoWritable();//Object that holds the word and it's position
        private IntWritable cluster = new IntWritable();//The cluster number

        /**
         * The method groups the words by the clusters they belong to. The key
         * is the cluster number and the value is an object of class
         * WordInfoWritable that contains the word and it's position.
         *
         * @param key key input, Object
         * @param value value input, Text
         * @param context the result of the method(K,V)
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            float[] centerArray = new float[dimensions];//Center position
            float[] wordArray = new float[dimensions];//Word position

            //Read and split the line
            String str = value.toString().replaceAll(",", "");
            str = str.replace("[", "");
            str = str.replace("]", "");
            str = str.replaceAll("\t", " ");
            String[] items = str.split(" ");

            //Find word's position
            IntWritable[] ints = new IntWritable[items.length - 1];
            IntArrayWritable array = new IntArrayWritable();
            for (int k = 1; k < items.length; k++) {
                ints[k - 1] = new IntWritable(Integer.parseInt(items[k]));
                wordArray[k - 1] = Float.parseFloat(items[k]);
            }
            array.set(ints);
            Text word = new Text();
            word.set(items[0]);

            //Create an object for the word
            wordInfo.set(word, array);

            double minDis = 1000;//Minimum distance
            double dis;//Current distance
            int clusterInt = 0;//Cluster number

            VectorOperations vo = new VectorOperations();
            try {
                //Open centers.txt file
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathCenters)));
                String lineWord = null;
                int j = 1;
                //For each center calculate the distance of the word and find the minimum
                while ((lineWord = br.readLine()) != null) {
                    String[] details = lineWord.split(" ");
                    for (int i = 0; i < details.length; i++) {
                        centerArray[i] = Float.parseFloat(details[i]);
                    }

                    dis = vo.distance(wordArray, centerArray);
                    if (dis < minDis) {
                        minDis = dis;
                        clusterInt = j;
                    }
                    j++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            cluster.set(clusterInt);
            context.write(cluster, wordInfo);
        }

    }

    /**
     * A Reducer class made for the second job of the assignment. Extends from
     * the class Reducer that is in hadoop libraries.It finds all the words that
     * are in the same cluster.
     */
    public static class job2Reducer extends Reducer<IntWritable, WordInfoWritable, IntWritable, Text> {

        /**
         * This method is called once for each cluster. It iterates the words
         * that belong to them (values),giving as an output the cluster number
         * and all the words.
         *
         * @param clusterNum key input, IntWritable
         * @param values value input, WordInfoWritable
         * @param context the result of the method(K,V)
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(IntWritable clusterNum, Iterable<WordInfoWritable> values, Context context) throws IOException, InterruptedException {

            VectorOperations vo = new VectorOperations();

            double[] centers = new double[dimensions];//New center

            //Create arraylist so that iteration more than once is possible
            ArrayList<WordInfoWritable> valuesList = new ArrayList<WordInfoWritable>();
            for (WordInfoWritable val : values) {
                WordInfoWritable w = new WordInfoWritable();
                w.set(val.getWord(), val.getArray());
                valuesList.add(w);
            }

            centers = vo.center(valuesList, dimensions);//Find new center

            //Replace old center with new in centers.txt file
            String newString = "";
            for (int i = 0; i < dimensions; i++) {
                newString = newString.concat(String.valueOf(centers[i]) + " ");
            }

            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathCenters)));
            List<String> newCenters = new ArrayList<String>();
            String lineWord;
            while ((lineWord = br.readLine()) != null) {
                newCenters.add(lineWord);
            }

            int k = clusterNum.get();//Get cluster number

            FileSystem fs1 = FileSystem.get(new Configuration());
            newCenters.set(k - 1, newString);
            FSDataOutputStream out = fs1.create(pathCenters, true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            for (String l : newCenters) {
                bw.write(l);
                bw.newLine();
            }
            bw.flush();
            bw.close();

            Text words = new Text();
            String wordS = "";

            //Get words and create a Text
            for (WordInfoWritable val : valuesList) {
                Text t = new Text(val.getWord());
                String s = t.toString();
                wordS = wordS.concat(s + " ");
            }
            words.set(wordS);

            context.write(clusterNum, words);

        }
    }

    public static void createCentersFile() throws IOException {
        Random r = new Random();
        List<String> randomCenters = new ArrayList<String>();//List of centers
        String newString = "";
        //Open centers.txt file
        FileSystem fs1 = FileSystem.get(new Configuration());

        FSDataOutputStream out = fs1.create(pathCenters, true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

        //For each center
        for (int j = 0; j < numOfCenters; j++) {
            //For each dimension
            for (int i = 0; i < dimensions; i++) {
                //Create a random float
                newString = newString.concat(String.valueOf(r.nextFloat() * (1 - 0)) + " ");
            }
            //Write center to file
            bw.write(newString);
            bw.newLine();
            newString = "";
        }
        bw.flush();
        bw.close();

    }

    public static void main(String[] args) throws Exception {

        pathCenters = new Path(args[4]);//Centers.txt file
        //Set distributedCache for the stopwords.txt file
        Configuration config1 = new Configuration();
        Job job1 = Job.getInstance(config1, "job1");
        DistributedCache.addCacheFile(new Path(args[3]).toUri(), job1.getConfiguration());
        job1.addCacheFile(new URI(args[3]));

        job1.setJarByClass(MRKmeans.class);
        job1.setMapperClass(FirstPartMapper.class);
        job1.setReducerClass(FirstPartReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntArrayWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        //Get the number of input files
        FileSystem fs = FileSystem.get(config1);
        Path pt = new Path(args[0]);
        ContentSummary cs = fs.getContentSummary(pt);
        long fileCount = cs.getFileCount();//FileCount is the number of input files
        dimensions = (int) fileCount;//Dimensions=number of dimensions on the vectors=number of input files

        job1.waitForCompletion(true);

        Path outPath = null;
        numOfCenters = Integer.parseInt(args[6]);//Get number of centers
        createCentersFile();//Set random centers
        int iterations = Integer.parseInt(args[5]);//Get number of iterations
        for (int i = 0; i < iterations; ++i) {
            outPath = new Path(args[2] + i);
            Job job2 = Job.getInstance(config1, "job2");
            job2.setJarByClass(MRKmeans.class);
            job2.setMapperClass(job2Mapper.class);
            job2.setReducerClass(job2Reducer.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(WordInfoWritable.class);

            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));

            FileOutputFormat.setOutputPath(job2, outPath);
            if (i == (iterations - 1)) {
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            } else {
                job2.waitForCompletion(true);
            }
        }
    }
}
