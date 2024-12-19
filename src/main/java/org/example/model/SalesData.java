package org.example.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Класс для хранения данных о выручке и количестве товаров
public class SalesData implements Writable, WritableComparable<SalesData> {

    private double revenue;
    private int quantity;

    public SalesData() {
    }

    public SalesData(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }

    public double getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return String.format("%.2f\t%d", revenue, quantity);
    }

    @Override
    public int compareTo(SalesData other) {
        return Double.compare(other.revenue, this.revenue);
    }
}
