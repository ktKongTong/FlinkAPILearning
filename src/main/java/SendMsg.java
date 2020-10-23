import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SendMsg {
    /*
    * 发送消息
    * */
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket=new ServerSocket(8887);
        System.out.println("服务端已启动，等待客户端连接..");
        Socket socket=serverSocket.accept();
        boolean in = true;
        Scanner scanner = new Scanner(System.in);
        OutputStreamWriter w = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        while (in){
            String input=scanner.nextLine();
            if(input.equals("")){
                in = false;
            }
            w.write(input+"\n");
            w.flush();
        }
        socket.shutdownOutput();
        socket.close();
    }
}
