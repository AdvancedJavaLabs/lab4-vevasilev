package org.example.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.model.SalesData;

import java.io.IOException;

// Mapper для подготовки агрегированных данных для дальнейшей сортировки
public class SalesSortMapper extends Mapper<Object, Text, SalesData, Text> {

    private static final int CATEGORY_INDEX = 0;
    private static final int REVENUE_INDEX = 1;
    private static final int QUANTITY_INDEX = 2;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 3) {
            String category = fields[CATEGORY_INDEX];
            double revenue = Double.parseDouble(fields[REVENUE_INDEX]);
            int quantity = Integer.parseInt(fields[QUANTITY_INDEX]);
            context.write(new SalesData(revenue, quantity), new Text(category));
        }
    }
}
