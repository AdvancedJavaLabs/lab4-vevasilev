package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.comparator.RevenueComparator;
import org.example.mapper.SalesMapper;
import org.example.mapper.SalesSortMapper;
import org.example.model.SalesData;
import org.example.reduce.SalesReducer;
import org.example.reduce.SalesSortReducer;

import java.io.IOException;

public class SalesAnalysis {

    private static final int INPUT_PATH_INDEX = 0;
    private static final int OUTPUT_PATH_INDEX = 1;
    private static final int SPLIT_SIZE_INDEX = 2;
    private static final int REDUCER_COUNT_INDEX = 3;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 4) {
            throw new IllegalArgumentException("Not enough arguments to run");
        }
        String inputPath = args[INPUT_PATH_INDEX];
        String outputPath = args[OUTPUT_PATH_INDEX];
        long splitSize = Long.parseLong(args[SPLIT_SIZE_INDEX]) * 1024 * 1024;
        int reducerCount = Integer.parseInt(args[REDUCER_COUNT_INDEX]);

        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(config);
        Path outputDir = new Path(outputPath);
        if (fileSystem.exists(outputDir)) {
            fileSystem.delete(outputDir, true);
        }
        Path outputDirTemp = new Path(outputPath + "_temp");
        if (fileSystem.exists(outputDirTemp)) {
            fileSystem.delete(outputDirTemp, true);
        }
        config.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);

        long startTime = System.currentTimeMillis();

        // Агрегация данных
        Job job1 = Job.getInstance(config, "Sales Aggregation");
        job1.setJarByClass(SalesAnalysis.class);
        job1.setMapperClass(SalesMapper.class);
        job1.setReducerClass(SalesReducer.class);
        job1.setNumReduceTasks(reducerCount);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(SalesData.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(SalesData.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "_temp"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Сортировка по выручке
        Job job2 = Job.getInstance(config, "Sales Sort");
        job2.setJarByClass(SalesAnalysis.class);
        job2.setMapperClass(SalesSortMapper.class);
        job2.setReducerClass(SalesSortReducer.class);
        job2.setNumReduceTasks(reducerCount);
        job2.setSortComparatorClass(RevenueComparator.class);
        job2.setMapOutputKeyClass(SalesData.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(SalesData.class);

        FileInputFormat.addInputPath(job2, new Path(outputPath + "_temp"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        long endTime = System.currentTimeMillis();
        System.out.println("Time: " + (endTime - startTime));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

