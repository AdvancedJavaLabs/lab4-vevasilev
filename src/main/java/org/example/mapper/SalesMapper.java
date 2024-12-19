package org.example.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.model.SalesData;

import java.io.IOException;

// Mapper для расчёта выручки и количества по категориям
public class SalesMapper extends Mapper<Object, Text, Text, SalesData> {

    private static final int TRANSACTION_ID_INDEX = 0;
    private static final int CATEGORY_INDEX = 2;
    private static final int PRICE_INDEX = 3;
    private static final int QUANTITY_INDEX = 4;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 5 && !fields[TRANSACTION_ID_INDEX].equals("transaction_id")) {
            String category = fields[CATEGORY_INDEX].trim();
            double price = Double.parseDouble(fields[PRICE_INDEX].trim());
            int quantity = Integer.parseInt(fields[QUANTITY_INDEX].trim());
            context.write(new Text(category), new SalesData(price * quantity, quantity));
        }
    }
}