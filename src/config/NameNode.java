package config;


import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.List;

public interface NameNode extends Remote {

    int PORT = 2001;

    /**
     * @return list of available servers ip
     */
    String[] getServersIp() throws RemoteException;

    /**
     * @return defaultChunkSize
     */
    long getDefaultChunkSize() throws RemoteException;

    void addFileData(String name, FileData fd) throws RemoteException;

    void removeFileData(String name) throws RemoteException;

    FileData retrieveFileData(String name) throws RemoteException;

    int getFileCount() throws RemoteException;

    List<String> getFileNames() throws RemoteException;

    Date getSaveDate() throws RemoteException;
}
