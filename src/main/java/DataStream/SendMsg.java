package DataStream;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

public class SendMsg {
    /*
    * 发送消息
    * */
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket serverSocket=new ServerSocket(8887);
        System.out.println("服务端已启动，等待客户端连接..");
        Socket socket=serverSocket.accept();
        OutputStreamWriter w = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
//        SendMsg.sendMsg(w);
        SendMsg.sendOrderMsg(w);

        socket.shutdownOutput();
        socket.close();
    }
    public static void sendMsg(OutputStreamWriter w) throws IOException {
        Scanner scanner = new Scanner(System.in);
        boolean in = true;
        while (in){
            String input=scanner.nextLine();
            if(input.equals("")){
                in = false;
            }
            w.write(input+"\n");
            w.flush();
        }
    }
    public static void sendOrderMsg(OutputStreamWriter w) throws IOException, InterruptedException {
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
    }
}
