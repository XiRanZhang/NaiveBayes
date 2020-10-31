package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

//calculate P(Ci)=the num of docs of class ci/the num of all docs
public class DocsNumOfEachClass extends Configured implements Tool {
    //we use type int to record the number of all .txt files,so the numbers of all files should less than Integer.MAX_VALUE= 2147483647

    static class DocsNumMaper extends Mapper<Text, BytesWritable,Text, IntWritable>{
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
             Text className = new Text();
             IntWritable one=new IntWritable(1);
             //read key from Sequencefile, the sequence file format is classname+filename
             String[] tokens=key.toString().split("\\+");
             className.set(tokens[0]);
             context.write(className,one);
        }
    }

    static class DocsNumReducer extends Reducer<Text,IntWritable,Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable val:values) {
                sum+=val.get();
            }
            IntWritable docsNumInEachClass=new IntWritable(sum);
            context.write(key,docsNumInEachClass);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();

        Path inputpath=new Path(conf.get("INPUTPATH"));
        Path outputpath=new Path(conf.get("OUTPUTPATH"));

        FileSystem fs=outputpath.getFileSystem(conf);
        //delete old output
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
        }

        Job job= Job.getInstance(conf,"DocsNumOfEachClass");
        job.setJarByClass(DocsNumOfEachClass.class);
        job.setMapperClass(DocsNumMaper.class);
        job.setCombinerClass(DocsNumReducer.class);
        job.setReducerClass(DocsNumReducer.class);
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass set map and reduce the same time.
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        //添加文件的输入路径
        SequenceFileInputFormat.addInputPath(job, inputpath);
        //添加文件的输出路径
        FileOutputFormat.setOutputPath(job, outputpath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
