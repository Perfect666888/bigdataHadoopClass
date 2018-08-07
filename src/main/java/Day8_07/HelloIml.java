package Day8_07;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author Perfect
 * @date 2018/8/7 14:51
 */
public class HelloIml extends UnicastRemoteObject implements IHello  {

    protected HelloIml() throws RemoteException {
    }

    @Override
    public String sayHello(String name) throws RemoteException {
        System.out.println(name);
        String line ="=====Hello==="+name+"=======";
        return line;
    }
}
