package org.example.models;

import java.io.Serializable;

public class Product implements Serializable {
    private int Id;
    private String Name;

    public int getId() {
        return Id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getQuantity() {
        return Quantity;
    }

    public void setQuantity(int quantity) {
        Quantity = quantity;
    }

    public Product(int id, String name, float price, int quantity) {
        Id = id;
        Name = name;
        this.price = price;
        Quantity = quantity;
    }

    private float price;
    private int Quantity;

}
