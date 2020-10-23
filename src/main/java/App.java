import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

public class App {
    /*
    * 发送订单数据socket
    * */
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket serverSocket=new ServerSocket(8888);
        System.out.println("服务端已启动，等待客户端连接..");
        Socket socket=serverSocket.accept();//侦听并接受到此套接字的连接,返回一个Socket对象

        OutputStreamWriter w = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        for(int i=0;i<1000;i++){
            Random random = new Random();
            int customId = random.nextInt(10)+1;
            int commodityId = random.nextInt(5)+1;
            float price = commodityId+1;
            long time = 1547718208 + i;
            String orderStr = i+1 + "," + customId + "," + commodityId + "," + price + "," + time;
            Thread.sleep(1000);
            System.out.println(orderStr);
            w.write(orderStr+"\n");
            w.flush();
        }
        socket.shutdownOutput();
        socket.close();
    }
}
