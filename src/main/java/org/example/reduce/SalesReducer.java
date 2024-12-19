package org.example.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.model.SalesData;

import java.io.IOException;

// Reducer для суммирования выручки и количества по категориям
public class SalesReducer extends Reducer<Text, SalesData, Text, SalesData> {

    @Override
    protected void reduce(Text key, Iterable<SalesData> values, Context context)
            throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        int totalQuantity = 0;
        for (SalesData value : values) {
            totalRevenue += value.getRevenue();
            totalQuantity += value.getQuantity();
        }
        context.write(key, new SalesData(totalRevenue, totalQuantity));
    }
}
