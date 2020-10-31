package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
//基于文件的InputFormat实现（通常是 FileInputFormat的子类） 默认行为是按照输入文件的字节大小，把输入数据切分成逻辑分块（logical InputSplit ）。
// 其中输入文件所在的FileSystem的数据块尺寸是分块大小的上限。下限可以设置mapred.min.split.size 的值。
//考虑到边界情况，对于很多应用程序来说，很明显按照文件大小进行逻辑分割是不能满足需求的。
// 在这种情况下，应用程序需要实现一个RecordReader来处理记录的边界并为每个任务提供一个逻辑分块的面向记录的视图。
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
    static class SequenceFileMapper extends
            Mapper<Text, BytesWritable, Text, BytesWritable> {

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }

    }


    @Override
    public int run(String[] strings) throws Exception {
        //应用程序至少应该指明输入/输出的位置（路径），并通过实现合适的接口或抽象类提供map和reduce函数。再加上其他作业的参数，就构成了作业配置（job configuration）
        Configuration conf=getConf();

        Path inputpath=new Path(conf.get("INPUTPATH"));
        Path outputpath=new Path(conf.get("OUTPUTPATH"));
        FileSystem fs=outputpath.getFileSystem(conf);
        //delete old output
        if(fs.exists(outputpath)){
            fs.delete(outputpath,true);
        }
        Job job=Job.getInstance(conf,"SmallFilesToSequenceFileConverter");
        if (job == null){
            return -1;
        }

        // run方法中指定了作业的几个方面
        //      1.命令行传递过来的输入/输出路径
        //      2.key/value的类型、
        //      3.输入/输出的格式等等J
        //      4.JobConf中的配置信息
        job.setJarByClass(SmallFilesToSequenceFileConverter.class);
        job.setMapperClass(SequenceFileMapper.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        SequenceFileOutputFormat.setOutputPath(job, outputpath);

        job.setInputFormatClass(WholeFileInputFormat.class);
        // 根据输入的训练集路径参数，来指定输入
        FileSystem trainFileSytem = inputpath.getFileSystem(conf);
        FileStatus[] trainfileStatus=trainFileSytem.listStatus(inputpath);
        if(trainfileStatus.length != 0){
            for (int i=0;i<trainfileStatus.length;i++){
                WholeFileInputFormat.addInputPath(job,trainfileStatus[i].getPath());
            }
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
