package Day8_07;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 * @author Perfect
 * @date 2018/8/7 14:56
 */
public class RmiServer {
    public static void main(String[] args) throws RemoteException, MalformedURLException {

        HelloIml hello = new HelloIml();

        //创建注册表
        LocateRegistry.createRegistry(12138);
        String rmiUrl="rmi://localhost:12138/RHello";
        Naming.rebind(rmiUrl,hello);

        System.out.println("绑定成功");


    }
}
