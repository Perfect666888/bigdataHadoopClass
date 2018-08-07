package Day8_07;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Perfect
 * @date 2018/8/7 14:49
 */
public interface IHello extends Remote {

    public String sayHello(String name) throws RemoteException;
}
