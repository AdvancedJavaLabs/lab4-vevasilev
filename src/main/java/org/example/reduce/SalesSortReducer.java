package org.example.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.model.SalesData;

import java.io.IOException;

// Reducer для вывода значение после сортировки по выручке
public class SalesSortReducer extends Reducer<SalesData, Text, Text, SalesData> {

    @Override
    protected void reduce(SalesData key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text category : values) {
            context.write(category, key);
        }
    }
}
