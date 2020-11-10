package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.*;

public class Evaluation {


    public Evaluation() throws IOException {
        //java 单机程序 评估分类效果
        //read the contents of input files into java maps
        Configuration configuration = new Configuration();

        //this path is true only when this is one spilt
        Path resultOfClassficationPath = new Path(Utils.RESULT_OF_CLASSFICATION+ "/part-r-00000" );
        FSDataInputStream inputStream = null;
        BufferedReader bfreader=null;
        FileSystem fileSystem = FileSystem.get(configuration);
        try{
            inputStream=fileSystem.open(resultOfClassficationPath);
            bfreader = new BufferedReader(new InputStreamReader(inputStream));
            String line=null;
            //用于计算每一类的TP
            //real class name;a[0]:the real number of the docs in the class
            //a[1]: the prediction number of the docs in the class
            //a[2]:the prediction and real class docs in the class
            HashMap<String,Integer[]> classifierMap=new HashMap<>();

            //read <classname,docNum> into hash map classPriorProbability
            while (( line = bfreader.readLine()) != null) {
                String[] tokens = line.split("\t");
                String realClass=tokens[0].split("\\+")[0];
                String classifierClass =tokens[1];

                //calculate precision here.
                if (realClass.equals(classifierClass)){
                    if (classifierMap.containsKey(realClass)){
                        Integer[] vals=classifierMap.get(realClass);
                        for (int i=0;i<vals.length;i++) vals[i] +=1;
                        classifierMap.put(realClass,vals);
                    }else {
                        Integer[] vals={1,1,1};
                        classifierMap.put(realClass,vals);
                    }
                }else{
                    if (classifierMap.containsKey(realClass)){
                        Integer[] vals=classifierMap.get(realClass);
                        vals[0] +=1;
                        classifierMap.put(realClass,vals);
                    }else {
                        Integer[] vals={1,0,0};
                        classifierMap.put(realClass,vals);
                    }
                    if (classifierMap.containsKey(classifierClass)){
                        Integer[] vals=classifierMap.get(classifierClass);
                        vals[1] +=1;
                        classifierMap.put(classifierClass,vals);
                    }else {
                        Integer[] vals={0,1,0};
                        classifierMap.put(classifierClass,vals);
                    }
                }

            }
            for (Integer val:classifierMap.get("AUSTR")){
                System.out.println(val);
            }
            for (Integer val:classifierMap.get("CANA")){
                System.out.println(val);
            }
            //use Macro-Average here
            double precision=0.0,recall=0.0;
            for (Map.Entry<String, Integer[]> entry : classifierMap.entrySet()) {
                Integer[] vals = entry.getValue();
                double tempPreci = (vals[2] + 0.0) / vals[1];
                precision+=tempPreci;
                double tempRecall = (vals[2] + 0.0) / vals[0];
                recall+=tempRecall;
                System.out.println(entry.getKey()+"\t"+"precision: "+tempPreci+"\t"+"recall: "+tempRecall);
            }
            recall =recall/classifierMap.size();
            precision =precision/classifierMap.size();
            System.out.println("precision: "+precision+"\t"+"recall: "+recall);
            double f1=2*(precision*recall)/(precision+recall);
            System.out.println("Macro-Avarage here: "+f1);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if (bfreader != null)
                bfreader.close();
            if (inputStream !=null)
                inputStream.close();
            if (fileSystem !=null)
                fileSystem.close();
        }
        }
}
