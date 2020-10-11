import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;


/*
    sensor_1,25,1547718199
    sensor_2,26,1547718199
    sensor_3,28,1547718199
    sensor_1,29,1547718200
    sensor_1,22,1547718201
    sensor_1,23,1547718202
    sensor_1,25,1547718203
    sensor_1,25,1547718204
    sensor_1,33,1547718205
    sensor_1,30,1547718206
    sensor_1,28,1547718207
    sensor_1,29,1547718208
    sensor_1,27,1547718209
    sensor_1,25,1547718230
*/

/*
sensor_1,20,1547718120
sensor_1,10,1547718130
sensor_1,9,1547718131
sensor_1,8,1547718132
sensor_1,9,1547718120
sensor_1,5,1547718135
sensor_1,9,1547718120
sensor_1,4,1547718136
sensor_1,9,1547718120
 */


public class SendTemp {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket=new ServerSocket(8887);
        System.out.println("服务端已启动，等待客户端连接..");
        Socket socket=serverSocket.accept();//侦听并接受到此套接字的连接,返回一个Socket对象
        boolean in = true;
        Scanner scanner = new Scanner(System.in);
        OutputStreamWriter w = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        while (in){
            System.out.println("input");
            String input=scanner.nextLine();
//            if(input.equals("")){
//                in = false;
//            }
            w.write(input+"\n");
            w.flush();
        }
        socket.shutdownOutput();
        socket.close();
    }
}


