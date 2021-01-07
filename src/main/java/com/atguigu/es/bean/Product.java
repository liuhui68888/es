package com.atguigu.es.bean;
/*************************************************
                时间: 2021-01-02
                作者: 刘  辉
                描述: 
  ************************************************/
public class Product {
    private Long id;//商品唯一标识
    private String title;//商品名称
    private Double price;//商品价格
    private String images;//图片地址

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getImages() {
        return images;
    }

    public void setImages(String images) {
        this.images = images;
    }

    public Product() {
    }

    public Product(Long id, String title, Double price, String images) {
        this.id = id;
        this.title = title;
        this.price = price;
        this.images = images;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", price=" + price +
                ", images='" + images + '\'' +
                '}';
    }
}
