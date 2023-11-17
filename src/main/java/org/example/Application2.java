package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.models.Product;

import java.util.ArrayList;
import java.util.List;

public class Application2 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP Spark DataSet").master("local[*]").getOrCreate();

        List<Product> products= new ArrayList<>();
        products.add(new Product(1,"ProDesk",1650,15));
        products.add(new Product(2,"Desktop",1850,19));
        products.add(new Product(3,"EliteBook",2950,35));
        products.add(new Product(4,"Pro MAC",9650,5));
        //Des encodeurs sont crées pour les Java Beans
        Encoder<Product> productEncoder= Encoders.bean(Product.class);
        //La difference entre le Dataset et le Dataframe est que les elements de Dataset sont typés(du meme type/objet)
        Dataset<Product> productDataset=ss.createDataset(products,productEncoder);
        productDataset.filter(new FilterFunction<Product>() {
            @Override
            public boolean call(Product product) throws Exception {
                return product.getQuantity()>17;
            }
        });
        productDataset.filter((FilterFunction<Product>) product -> product.getPrice()>2800);
        productDataset.show();
    }
}
