using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using MessagePack;

namespace postgretest;

public class Util
{
    public static IPEndPoint? IpStringToIP(string s)
    {
        var splitted = s.Split(":");
        var port = int.Parse(splitted[1]);
        var hostname = splitted[0];
        Console.WriteLine(hostname);
        try
        {
            IPHostEntry host = Dns.GetHostEntry(hostname);
            return new IPEndPoint(host.AddressList[0], port);
        }catch(SocketException)
        {
            Console.WriteLine("Failed to resolve "+hostname);
            return null;

        }

    }

    public static readonly string AppFolder = "";

    public static CancellationTokenSource AsyncTimeout(int timeout, CancellationToken ct)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task.Run(() => Task.Delay(timeout, ct)).ContinueWith(task=>
        {
            cts.Cancel();
        });
        return cts;
    }
    public static void ReportError(Task task)
    {
        if (!task.IsCanceled && task.IsFaulted)
        {
            Console.WriteLine(task.Exception);
        }

        if (task.IsCanceled)
        {
            Console.WriteLine("canceled task");
        }
    }

    static Util()
    {
        string appdata_path = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
        if (appdata_path != "")
        {
            AppFolder = Path.Combine(appdata_path, "RaftProject");
        }
        else
        {
            AppFolder = "/RaftProject";
        }
    }

    public static async Task<bool> ReadExactlyAsync(Stream reader, byte[] buffer, int count, int timeout, CancellationToken ct)
    {
        if (timeout != -1)
        {
            using (CancellationTokenSource cts = Util.AsyncTimeout(timeout, ct))
            {

                try
                {
                    await reader.ReadExactlyAsync(buffer, 0,count, cts.Token);
                }
                catch (OperationCanceledException e)
                {
                    if (ct.IsCancellationRequested)
                    {
                        throw;
                    }

                    return false;
                }
                catch (Exception)
                {
                    return false;
                }
            }
            
        }
        else
        {
            try
            {
                await reader.ReadExactlyAsync(buffer, 0,count, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception)
            {
                return false;
            }
        }

        return true;
    }
    public static async Task<T?> Deserialize<T>(Stream reader, int timeout,CancellationToken ct) where T: Packet
    {
        
        byte[] size_buffer= new byte[sizeof(int)];
        bool success = await Util.ReadExactlyAsync(reader,size_buffer,  sizeof(int),timeout, ct);
        if (!success)
        {
            return null;
        }            

        int size = 0;
        if(BitConverter.IsLittleEndian)
        {
            size = BitConverter.ToInt32(size_buffer.Reverse().ToArray());
        }
        else
        {
            size = BitConverter.ToInt32(size_buffer);
        }

        if (size > 1000)
        {
            Console.WriteLine("we are under attack by a ddos");
        }

        byte[] buffer = new byte[size];
        bool success2 = await Util.ReadExactlyAsync(reader,buffer,  size,timeout,ct);
        if (!success2)
        {
            return null;
        }

        try
        {
            return MessagePackSerializer.Deserialize<T>(buffer);
        }
        catch (Exception)
        {
            return null;
        }
    }
    
}