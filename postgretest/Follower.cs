using System.Net;
using System.Net.Sockets;

namespace postgretest;

public class Follower: IDisposable
{
    public int port;
    public string hostname;
    public TcpClient connection = new TcpClient();
    public int id;
    public bool debugBlock = false;
    private bool disposed = false;

    public Follower(string _hostname,int _port, int _id)
    {
        hostname = _hostname;
        port = _port;
        id = _id;
    }

    public async Task<T?> AsyncReadPacket<T>(CancellationToken ct,int timeout) where T: Packet 
    {
        try
        {

            return await Util.Deserialize<T>(connection.GetStream(), timeout, ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch(Exception e)
        {
            if (await Connect(timeout,ct))
            {
                try
                {

                    return await Util.Deserialize<T>(connection.GetStream(), timeout, ct);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch
                {
                    
                }
            }
            return null;
        }
    }
    public async Task<bool> AsyncWrite(byte[] data,int timeout,CancellationToken ct)
    {
        if (debugBlock)
        {
            return false;
        }

        try
        {
            var stream = connection.GetStream();
            stream.WriteTimeout = timeout;
            await stream.WriteAsync(data, ct);
            return true;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine("connecting to "+hostname+":"+port);
            if (await Connect(timeout,ct))
            {
                try
                {
                    var stream = connection.GetStream();
                    stream.WriteTimeout = timeout;
                    await stream.WriteAsync(data, ct);
                    return true;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception er)
                {
                    return false;
                }
            }

            Console.WriteLine("failed to connect to "+hostname+":"+port);
            return false;
        }
    }

    private async Task<bool> Connect(int timeout, CancellationToken ct)
    {
        if (debugBlock)
        {
            return false;
        }

        connection.Dispose();
        connection = new TcpClient();
        try
        {
            var delay = Task.Delay(timeout, ct);
            Task connect = connection.ConnectAsync(hostname, port, ct).AsTask();
            await Task.WhenAny(connect,delay);
        }
        catch (OperationCanceledException e)
        {
            throw;
        }
        catch (Exception e)
        {
            return false;
        }

        return true;

    }
    protected virtual void Dispose(bool disposing)
    {
        if (!disposed && disposing)
        {
            connection.Dispose();
            disposed = true;
        }         
    }
    public void Dispose()
    {
        Dispose(true);
        
        GC.SuppressFinalize(this);
    }

    ~Follower()
    {
        Dispose(false);
    }
}