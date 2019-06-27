package com.batch.hadoopcompatibility;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.core.fs.Path;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/6/27 16:08
 */
public class FlinkHadoopDemoJava {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<LongWritable, Text>> input =
                env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(),
                        LongWritable.class, Text.class, ""));

    }
}
