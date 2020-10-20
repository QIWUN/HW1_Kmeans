import java.io.IOException;
import java.util.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.Month;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class Kmeans {
  
 public static class KMap extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    //record 4 center
    public List<String> preKey = new ArrayList<>();
    public List<Integer> preValue = new ArrayList<>();

    public void setup(Context context) {
        String sCounter = context.getConfiguration().get("COUNTER");
        int nCounter = Integer.parseInt(sCounter);
        //20201020
        if(nCounter == 1){
            String new_center = context.getConfiguration().get("NEW_CENTER");
            String line = new_center.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String new_key = tokenizer.nextToken();
                String token = tokenizer.nextToken(); 
                int new_value = Integer.parseInt(token);
                preKey.add(new_key);
                preValue.add(new_value);
            }
        }else{
            preKey.add("K1");
            preKey.add("K2");
            preKey.add("K3");
            preKey.add("K4");
            preValue.add((int)(Math.random() * 120 + 1));
            preValue.add((int)(Math.random() * 120 + 1));
            preValue.add((int)(Math.random() * 120 + 1));
            preValue.add((int)(Math.random() * 120 + 1));
        }

    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String new_key = tokenizer.nextToken(); //時間

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken(); 
            int new_value = Integer.parseInt(token);

            List<Double> list_distances = new ArrayList<>();

            //計算距離
            for(int i=0;i<preValue.size();i++){
                double distance = Math.sqrt(Math.pow((new_value - preValue.get(i)),2));
                list_distances.add(distance);
            }
            //找出最近的中心點並分類
            for(int i=0;i<preValue.size();i++){
                if(Collections.min(list_distances) == Math.sqrt(Math.pow((new_value - preValue.get(i)),2))){
                    context.write(new Text(preKey.get(i)), new IntWritable(new_value));
                }
            }
            
        }       
        
    }

    public void cleanup(Context context) throws IOException, InterruptedException{

    }

 }

 public static class KReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void setup(Context context) {

    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum =0;
        int count =0;
        for (IntWritable val : values) {
            sum+=val.get();
            count++;
        }
        int new_value = sum/count;
        //得到該類別的中心點
        context.write(key, new IntWritable(new_value));
    }

    public void cleanup(Context context) throws IOException, InterruptedException{

    }

 }

 public static void main(String[] args) throws Exception {

    //先做第一次產生初始中心點檔案
    String flag1 = "NEW_CENTER";
    String flag2 = "COUNTER";
    String isFisrt = "0";
    String fileName = "/part-r-00000";

    //JOB CONF
    Configuration conf = new Configuration();
    Path inputFile = new Path(args[0]);
    Path outputFile = new Path(args[1]);

    /*20201020*/
    conf.set(flag2, isFisrt);

    Job job1 = new Job(conf, "kmeans"); 
    job1.setSpeculativeExecution(false);
    job1.setMapSpeculativeExecution(false);
    job1.setReduceSpeculativeExecution(false);

    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputFile)) {
        hdfs.delete(outputFile, true);
    }

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setMapperClass(KMap.class);
    job1.setReducerClass(KReduce.class);
    job1.setJarByClass(Kmeans.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job1, inputFile);
    FileOutputFormat.setOutputPath(job1, outputFile);
    job1.waitForCompletion(true);
    isFisrt = "1";
    
    /* 20201020 */
    int repeated = 0;  
    do{
        
        Center center = new Center();
        String oldCenterValues = center.printCenter(new Path(outputFile + fileName));
        System.out.println(oldCenterValues);

        conf.set(flag1, oldCenterValues);
        conf.set(flag2, isFisrt);

        Job job = new Job(conf, "kmeans"); 
        job.setSpeculativeExecution(false);
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);

        //FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputFile)) {
            hdfs.delete(outputFile, true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(KMap.class);
        job.setReducerClass(KReduce.class);
        job.setJarByClass(Kmeans.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);
        job.waitForCompletion(true);

        center = new Center();  
        oldCenterValues = center.printNewCenter(outputFile);  
        System.out.println(oldCenterValues);

        repeated++;

    }while(repeated < 30);
    
  }

}
