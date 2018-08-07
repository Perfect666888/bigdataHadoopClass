package Day8_07;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * @author Perfect
 * @date 2018/8/7 15:01
 */
public class RmiClient {

    public static void main(String[] args) throws RemoteException, NotBoundException, MalformedURLException {
        IHello hello = (IHello) Naming.lookup("rmi://localhost:12138/RHello");
        System.out.println(hello.sayHello("bigdata"));
    }
}
