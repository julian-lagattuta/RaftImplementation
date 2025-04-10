using System.Net;
using System.Net.Sockets;

namespace postgretest;

public enum CommittedState
{
    Unknown,
    NeverWillBeCommitted,
    Committed
}
public record OperationStatus(
    LogEntry? entry,
    CommittedState state
)
{
    public override string ToString()
    {
        return $"{nameof(entry)}: {entry}, {nameof(state)}: {state}";
    }
}

public class User
{
    private List<IPEndPoint> server_ips;

    User(List<IPEndPoint> _server_ips)
    {
        server_ips = _server_ips;
    }

    public async Task<OperationStatus> DoOperation(Operation op,CancellationToken ct=  default)
    {
        int id = Random.Shared.Next(server_ips.Count);
        var server_ip = server_ips[id];
        return await DoOperation(op, server_ip,ct);

    }
    private async Task<OperationStatus> DoOperation(Operation op, IPEndPoint server_ip, CancellationToken ct = default)
    {

        var message = new DoOperation(op);
        CommittedState committedState = CommittedState.NeverWillBeCommitted;
        LogEntry? entry  = null;
        try
        {
            TcpClient server = new TcpClient();
            await server.ConnectAsync(server_ip,ct);
            var server_stream = server.GetStream();

            await server_stream.WriteAsync(message.Serialize<DoOperation>(), ct);
            committedState = CommittedState.Unknown;
            UserResponse? response = await Util.Deserialize<UserResponse>(server_stream, -1, ct);
            switch (response)
            {
                case null:
                    return new OperationStatus(null, CommittedState.Committed);
                    break;
                case UserFinalResponse userFinalResponse:
                    return new OperationStatus(userFinalResponse.entry,CommittedState.Committed);
                    break;
                case UserImmediateResponse userImmediateResponse:
                    entry = userImmediateResponse.entry;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(response));
            }
            UserResponse? second_response = await Util.Deserialize<UserResponse>(server_stream, -1, ct);
            switch (second_response)
            {
                case null:
                    return new OperationStatus(null,CommittedState.Unknown);
                    break;
                case UserFinalResponse userFinalResponse:
                    return new OperationStatus(userFinalResponse.entry, CommittedState.Committed);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(second_response));
            }
        }
        catch(SocketException se)
        {
            Console.WriteLine("DEBUG DELETE THIS LATER: ",se);
            return new OperationStatus(null,CommittedState.NeverWillBeCommitted);
        }
    
    }

    public void LoadIps(string filename)
    {
        server_ips = File.ReadLines(filename).Select(IPEndPoint.Parse).ToList();
    }

    public static async Task UserMainAsync()
    {
        try
        {

            User user = new User(null);
            user.LoadIps("""C:\Users\kidsf\RiderProjects\RaftImplementation\RaftImplementation\user-ips.txt""");
            Console.WriteLine(user.server_ips);

            while (true)
            {
                var id = Console.ReadLine();
                var int_id = int.Parse(id);
                var value = await user.DoOperation(new Write("obama", "bidenama"), user.server_ips[int_id]);
                Console.WriteLine(value);


            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }
    public static void UserMain()
    {
        UserMainAsync().Wait();
        
    }
}