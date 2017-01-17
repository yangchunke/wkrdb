package net.yck.wrkdb.client;

import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import net.yck.wrkdb.service.thrift.DbService;

public class ThriftDbClient extends DbService.Client implements AutoCloseable {

  private final static int DEF_SOCKET_TIMEOUT  = (int) TimeUnit.SECONDS.toMillis(15);
  private final static int DEF_CONNECT_TIMEOUT = (int) TimeUnit.MILLISECONDS.toMillis(500);
  private final TTransport transport;

  private ThriftDbClient(TProtocol prot) {
    super(prot);
    transport = prot.getTransport();
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }

  public static ThriftDbClient getClient(String host, int port) throws SocketException, TTransportException {
    return getClient(host, port, DEF_SOCKET_TIMEOUT, DEF_CONNECT_TIMEOUT);
  }

  public static ThriftDbClient getClient(String host, int port, int socketTimeout, int connectTimeout)
      throws SocketException, TTransportException {
    TSocket socket = new TSocket(host, port, socketTimeout, connectTimeout);
    socket.getSocket().setReuseAddress(true);
    socket.getSocket().setSoLinger(true, 0);
    socket.open();
    return new ThriftDbClient(new TBinaryProtocol(socket));
  }

}
