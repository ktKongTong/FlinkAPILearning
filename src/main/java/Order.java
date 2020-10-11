public class Order{
    private Integer orderId;
    private Integer customId;
    private Integer commodityId;
    private Float price;
    private Long time;
    public Order(Integer orderId,Integer customId,Integer commodityId,Float price,Long time){
        this.orderId = orderId;
        this.customId = customId;
        this.commodityId = commodityId;
        this.price = price;
        this.time = time;
    }
    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public Integer getCustomId() {
        return customId;
    }

    public void setCustomId(Integer customId) {
        this.customId = customId;
    }

    public Integer getCommodityId() {
        return commodityId;
    }

    public void setCommodityId(Integer commodityId) {
        this.commodityId = commodityId;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return orderId +
                "," + customId +
                "," + commodityId +
                "," + price +
                "," + time ;
    }
}
