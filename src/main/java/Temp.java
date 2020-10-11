public class Temp {
    private String tempId;
    private Integer temp;
    private Long time;

    public Temp(String s){
        String[] strings=s.split(",");
        this.tempId = strings[0];
        this.temp = Integer.parseInt(strings[1]);
        this.time = Long.parseLong(strings[2]);
    }

    public Temp(String tempId,Integer temp,Long time){
        this.tempId = tempId;
        this.temp = temp;
        this.time = time;
    }

    public String getTempId() {
        return tempId;
    }

    public void setTempId(String tempId) {
        this.tempId = tempId;
    }

    public Integer getTemp() {
        return temp;
    }

    public void setTemp(Integer temp) {
        this.temp = temp;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Temp{" +
                "tempId='" + tempId + '\'' +
                ", temp=" + temp +
                ", time=" + time +
                '}';
    }
}
