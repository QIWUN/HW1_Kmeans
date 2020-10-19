import java.io.IOException;
import java.util.*;
import static java.lang.System.out;
import java.util.regex.*;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.BufferedWriter;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Kmeans {

    public static class DefultMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // one data per row
            List<String> tdata = new ArrayList<>(); // all feature
            StringTokenizer st = new StringTokenizer(value.toString().trim(),",");
            while(st.hasMoreElements())
            {
                // Check Is Numeric
                String chkS = st.nextToken();
                if (isNumeric(chkS) ) {
                    tdata.add(chkS);
                }
            }
            Configuration conf = context.getConfiguration();
            String k = conf.get("Kmeans");
            Random rand = new Random();
            String tkey = String.valueOf(rand.nextInt(Integer.parseInt(k)));
            String tvalue = tdata.get(rand.nextInt(tdata.size()));
            context.write(new Text(tkey), new IntWritable(Integer.parseInt(tvalue)));
        }
    }

    public static class DefultReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        //MapWritable map = new MapWritable (); // k feature
        private Map<String,IntWritable> map = new HashMap(); // k feature
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // only one input
            List<IntWritable> tdata = new ArrayList<>(); // all feature
            for (IntWritable val : values) {
                tdata.add(new IntWritable(val.get()));
            }
            Random rand = new Random();
            map.put("k"+ String.valueOf(map.size()) , new IntWritable(rand.nextInt(tdata.size())));
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String,IntWritable> m : map.entrySet()) {
                context.write(new Text(m.getKey()), new IntWritable(m.getValue().get()));
            }
        }
    }



 public static class KMap extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    //discord 4 centers
    private List<String> centroDate = new ArrayList<>();
    private List<Integer> centroValue = new ArrayList<>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String new_key = tokenizer.nextToken(); //time

        //initital 4 center
        centroDate.add("K1");
        centroDate.add("K2");
        centroDate.add("K3");
        centroDate.add("K4");

        // get pre-result
        Configuration conf = context.getConfiguration();
        centroValue.add(26);
        centroValue.add(75);
        centroValue.add(62);
        centroValue.add(52);
        //centroValue.add(Integer.parseInt(conf.get("k1")));
        //centroValue.add(Integer.parseInt(conf.get("k2")));
        //centroValue.add(Integer.parseInt(conf.get("k3")));
        //centroValue.add(Integer.parseInt(conf.get("k4")));

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken(); 
            int new_value = Integer.parseInt(token);

            List<Double> list_distances = new ArrayList<>();

            //calculate distance
            for(int i=0;i<centroValue.size();i++){
                double distance = Math.sqrt(Math.pow((new_value - centroValue.get(i)),2));
                list_distances.add(distance);
            }
            //find center and do kind
            for(int i=0;i<centroValue.size();i++){
                if(Collections.min(list_distances) == Math.sqrt(Math.pow((new_value - centroValue.get(i)),2))){
                    context.write(new Text(centroDate.get(i)), new IntWritable(new_value));
                }
            }
            
        }       
        
    }
 }

 public static class KReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<String,IntWritable> map = new HashMap(); // k feature
    private int sum =0;
    private int count =0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {    
        for (IntWritable val : values) {
            sum+=val.get();
            count++;
        }
        //get center
        map.put("k"+ String.valueOf(map.size()) , new IntWritable(sum/count));
        //context.write(new Text("k" + key), new IntWritable(sum/count));
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String,IntWritable> m : map.entrySet()) {
            context.write(new Text(m.getKey()), new IntWritable(m.getValue().get()));
        }
    }
 }

    public static void main(String[] args) throws Exception {
        // k = temp file counts
        int k = 4;

        // use DefuleMap，DefuleReduce random initital
        // K1 00(value)
        // K2 00(value)
        // K3 00(value)
        // .....
        Configuration conf = new Configuration();
        conf.set("Kmeans",String.valueOf(k));
        Job job = new Job(conf, "DefuleKmeans");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(DefultMap.class);
        job.setReducerClass(DefultReduce.class);
        job.setJarByClass(Kmeans.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);*/

        // use KMap，KReduce get temp center
        // in KMap cluster, KReduce calculate center
        for (int i = 0 ; i < 0; i++) 
        {
            // get result.pm25 
            Path resultPath = null;
            if (i==0) {
                resultPath = new Path(args[1]+ "/part-r-0000" + String.valueOf(i));
            }
            else {
                resultPath = new Path((String.valueOf(i) + args[1]+ "/part-r-0000" + String.valueOf(i)));
            }

            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream fsIn = hdfs.open(new Path(args[1]+ "/part-r-00000"));
            LineReader in = new LineReader(fsIn, conf);
            Text line = new Text();
            // get one text
            String tkey = "";
            String tvalue = "";
            System.out.println("\n第" + String.valueOf(i) + "次 計算");
            while(in.readLine(line) > 0)
            {
                StringTokenizer st = new StringTokenizer(line.toString().trim(),",");
                while(st.hasMoreElements())
                {
                // Check Is Numeric
                    String chkS = st.nextToken();
                    if (isNumeric(chkS)) {
                        tvalue = chkS;
                    }
                    else {
                        tkey = chkS;
                    }
                }
                conf.set(tkey,tvalue);
                System.out.println(tkey + ":" + tvalue);
            }
            // true stands for recursively deleting the folder you gave
            // if output exist => delete
            if (hdfs.exists(new Path(args[1]))) {
               hdfs.delete(new Path(args[1]), true);//true means delete anyway
            }

            System.out.println("Kjob開始");
            Job kjob = new Job(conf, "Kmeans");
            kjob.setOutputKeyClass(Text.class);
            kjob.setOutputValueClass(IntWritable.class);
            kjob.setMapperClass(KMap.class);
            kjob.setReducerClass(KReduce.class);
            kjob.setJarByClass(Kmeans.class);
            kjob.setInputFormatClass(TextInputFormat.class);
            kjob.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(kjob, new Path(args[0]));
            FileOutputFormat.setOutputPath(kjob, new Path(args[1]));
            kjob.waitForCompletion(true);
            System.out.println("Kjob結束");
        }
    }

    // Check Is Numeric
    public static boolean isNumeric(String str){
           Pattern pattern = Pattern.compile("[0-9]*");
           Matcher isNum = pattern.matcher(str);           
           if( !isNum.matches() ){               
               return false;
           }  return true;
    }
    
}