import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import king.Utils.Distance;
import king.Utils.ListWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * KNearestNeigbour Classifier
 * each instance in training set is of form a1,a2,a3...an,l1
 * in which l1 represents the label. and each instance in 
 * predict set is of form a1,a2,a3...an,-1,in which -1 is the 
 * label we want to specify.
 * In my algorithm,I assume that the trainning set is relatively
 * small so we can load them in memory and the predict set is large
 * another thing we need to pay attention to is that all our test 
 * instances are all in one file so that the index of line is unique
 * to each instance.
 * @author KING
 *
 */
public class KNearestNeighbour {
    public static class KNNMap extends Mapper<LongWritable,
            Text,LongWritable,ListWritable<Text>>{
        private int k;
        private ArrayList<Instance> trainSet;
        private Configuration conf;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException{
            conf = context.getConfiguration();
            k = context.getConfiguration().getInt("k", 1);
            trainSet = new ArrayList<Instance>();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            Path patternsPath1 = new Path(patternsURIs[0].getPath());
            String patternsFileName1 = patternsPath1.getName().toString();

//            Path[] trainFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            System.out.println("trainFile:"+trainFile);
            //add all the tranning instances into attributes
            BufferedReader br = null;
            String line;
            for(int i = 0;i <  patternsURIs.length-1;i++){
//                br = new BufferedReader(new FileReader(trainFile[0].toString()));
                br = new BufferedReader(new FileReader(patternsFileName1));
                System.out.println("br:"+br);
                String line1 = br.readLine();
                if(line1 == null){
                    System.out.println("null!!!");
                }
                while((line = br.readLine()) != null){

                    Instance trainInstance = new Instance(line);
//                    System.out.println("trainInstance:"+trainInstance);
                    trainSet.add(trainInstance);
                }
            }
        }

        /**
         * find the nearest k labels and put them in an object
         * of type ListWritable. and emit <textIndex,lableList>
         */
        @Override
        public void map(LongWritable textIndex, Text textLine, Context context)
                throws IOException, InterruptedException {
            //distance stores all the current nearst distance value
            //. trainLable store the corresponding lable
            ArrayList<Double> distance = new ArrayList<Double>(k);
            ArrayList<Text> trainLable = new ArrayList<Text>(k);
            for(int i = 0;i < k;i++){
                distance.add(Double.MAX_VALUE);
                trainLable.add(new Text("null"));
            }
            ListWritable<Text> lables = new ListWritable<Text>(Text.class);
            Instance testInstance = new Instance(textLine.toString());
            for(int i = 0;i < trainSet.size();i++){
                try {
                    double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
                    int index = indexOfMax(distance);
                    if(dis < distance.get(index)){
                        distance.remove(index);
                        trainLable.remove(index);
                        distance.add(dis);
                        trainLable.add(new Text(trainSet.get(i).getLable()));
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            lables.setList(trainLable);
//            System.out.println("map:labels： "+trainLable);
            context.write(textIndex, lables);
        }

        /**
         * return the index of the maximum number of an array
         * @param array
         * @return
         */
        public int indexOfMax(ArrayList<Double> array){
            int index = -1;
            Double min = Double.MIN_VALUE;
            for (int i = 0;i < array.size();i++){
                if(array.get(i) > min){
                    min = array.get(i);
                    index = i;
                }
            }
            return index;
        }
    }

    public static class KNNReduce extends Reducer<LongWritable,ListWritable<Text>,NullWritable,Text>{
        List<String> PredictSet = new ArrayList<String>();
        private Configuration conf;
        @Override
        protected void setup(Reducer.Context context) throws IOException,InterruptedException{
            conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            Path patternsPath1 = new Path(patternsURIs[1].getPath());
            String patternsFileName1 = patternsPath1.getName().toString();

//            Path[] trainFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            System.out.println("trainFile:"+trainFile);
            //add all the tranning instances into attributes
            BufferedReader br = null;
            String line;
            for(int i = 0;i <  patternsURIs.length-1;i++){
//                br = new BufferedReader(new FileReader(trainFile[0].toString()));
                br = new BufferedReader(new FileReader(patternsFileName1));
                while((line = br.readLine()) != null){
                    Instance trainInstance = new Instance(line);
//                    System.out.println("trainInstance:"+trainInstance);
                    PredictSet.add(trainInstance.getLable());
                }
            }
        }
        private int sum = 0;
        private int right = 0;
        @Override
        public void reduce(LongWritable index, Iterable<ListWritable<Text>> kLables, Context context)
                throws IOException, InterruptedException{
            /**
             * each index can actually have one list because of the
             * assumption that the particular line index is unique
             * to one instance.
             */

//            System.out.println("sum:"+sum);
            Text predictedLable = new Text();
            for(ListWritable<Text> val: kLables){
                try {
                    predictedLable = valueOfMostFrequent(val);
                    break;
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            context.write(NullWritable.get(), predictedLable);

            if(predictedLable.toString().equals(PredictSet.get(sum))){
//                System.out.println("sum:"+sum+"righr"+right);
                right += 1;
            }
            sum += 1;
            if(sum == 45){
                double accuracy = Double.valueOf(right) / Double.valueOf(sum);
                System.out.println("sum:"+sum+"righr"+right+"accuracy"+accuracy);
                context.write(NullWritable.get(),new Text(String.valueOf(accuracy)));
            }

        }

        public Text valueOfMostFrequent(ListWritable<Text> list) throws Exception{
            if(list.isEmpty())
                throw new Exception("list is empty!");
            else{
                HashMap<Text,Integer> tmp = new HashMap<Text,Integer>();
                for(int i = 0 ;i < list.size();i++){
                    if(tmp.containsKey(list.get(i))){
                        Integer frequence = tmp.get(list.get(i)) + 1;
                        tmp.remove(list.get(i));
                        tmp.put(list.get(i), frequence);
                    }else{
                        tmp.put(list.get(i), new Integer(1));
                    }
                }
                //find the value with the maximum frequence.
                Text value = new Text();
                Integer frequence = new Integer(Integer.MIN_VALUE);
                Iterator<Entry<Text, Integer>> iter = tmp.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Text,Integer> entry = (Map.Entry<Text,Integer>) iter.next();
                    if(entry.getValue() > frequence){
                        frequence = entry.getValue();
                        value = entry.getKey();
                    }
                }
                return value;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        System.out.println(args[0]+" "+args[1]+" "+args[2]+" "+args[3]);

        Job kNNJob = new Job();
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        kNNJob.setJobName("kNNJob");
        kNNJob.setJarByClass(KNearestNeighbour.class);
        kNNJob.addCacheFile(new Path(remainingArgs[2]).toUri());
        kNNJob.addCacheFile(new Path(remainingArgs[0]).toUri());
//        DistributedCache.addCacheFile(URI.create(args[2]), kNNJob.getConfiguration());
        kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));

        kNNJob.setMapperClass(KNNMap.class);
        kNNJob.setMapOutputKeyClass(LongWritable.class);
        kNNJob.setMapOutputValueClass(ListWritable.class);

        kNNJob.setReducerClass(KNNReduce.class);
        kNNJob.setOutputKeyClass(NullWritable.class);
        kNNJob.setOutputValueClass(Text.class);

        kNNJob.setInputFormatClass(TextInputFormat.class);
        kNNJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
        kNNJob.waitForCompletion(true);


        System.out.println("finished!");
    }
}