package org.example.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.example.model.SalesData;

// Кастомный компаратор для сортировки по выручке
public class RevenueComparator extends WritableComparator {

    protected RevenueComparator() {
        super(SalesData.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        SalesData s1 = (SalesData) o1;
        SalesData s2 = (SalesData) o2;
        return Double.compare(s2.getRevenue(), s1.getRevenue());
    }
}