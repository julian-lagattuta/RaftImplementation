using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using MessagePack;
using Npgsql.Replication.TestDecoding;
using postgretest;

class RaftServer: IDisposable
{
    public static int HEARTBEATMS = 1000;
    private Log log;
    private List<Follower> followers = new List<Follower>();
    public IPEndPoint server_ip;
    public int commitIndex = 0;
    public PersistentStorage persistentStorage;
    public int myId;
    private Stopwatch stopwatch = new Stopwatch();
    private long lastHeartbeat;

    private Func<Operation,Task> commitFunc; 
    private CancellationTokenSource raft_cts= new CancellationTokenSource();
    private CancellationTokenSource old_cts = null;

    public int? currentLeaderId;
    public int[] nextIndex;
    public int[] matchIndex;
    public RaftMode mode = RaftMode.Follower;
    private ReaderWriterLock stateLock = new ReaderWriterLock();

    private bool disposed = false;

    private SemaphoreSlim clientsWaitingLock = new SemaphoreSlim(1,1);
    private List<(TcpClient, int)> clientsWaiting = new List<(TcpClient, int)>();
    private IPEndPoint user_server_ip;
    
    //Returns true if successfully changed. Returns false if there was a race condition and the mode is outdated.
    private bool changeMode(RaftMode _mode, CancellationToken old_ct,int? newLeaderId = null,int? newTerm = null )
    {
        
        stateLock.AcquireWriterLock(100);
        try
        {

            if (old_ct != raft_cts.Token)
            {
                Console.WriteLine("funny race condition happened and prevented");
                return false;
            }

            mode = _mode;
            old_cts = raft_cts;
            raft_cts = new CancellationTokenSource();
            old_cts.Cancel();
            Console.WriteLine("changing mode to " + _mode.ToString());
            switch (_mode)
            {
                case RaftMode.RunningElection:
                    break;
                case RaftMode.Leader:
                    nextIndex = Enumerable.Repeat(log.Length + 1, followers.Count).ToArray();
                    break;
                case RaftMode.Follower:
                    lastHeartbeat = stopwatch.ElapsedMilliseconds;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }

            currentLeaderId = newLeaderId;
            if (newTerm is not null)
            {
                persistentStorage.SetTermMax(newTerm.Value,CancellationToken.None).Wait();
            }
            return true;
        }
        finally
        {
            stateLock.ReleaseWriterLock();
        }
    }
    public RaftServer(int server_port, int client_port, Func<Operation,Task> _commitFunc)
    {
        commitFunc = _commitFunc;
        stopwatch.Start();
        server_ip = new IPEndPoint(IPAddress.Any, server_port);
        user_server_ip = new IPEndPoint(IPAddress.Any, client_port);
        myId = int.Parse(Environment.GetEnvironmentVariable("id"));
        
        if (!Directory.Exists(Util.AppFolder))
        {
            throw new Exception("not found path " + Util.AppFolder);
        }

        string ip_list_path = Path.Combine(Util.AppFolder, "ips.txt");
        
        var ips = File.ReadLines(ip_list_path);
        
        int i = -1;
        foreach(var ipstring in ips)
        {
            i++;
            if (i == myId)
            {
                followers.Add(null);
                continue;
            }
            var splitted = ipstring.Split(":");
            var follower_port = int.Parse(splitted[1]);
            followers.Add(new Follower(splitted[0],follower_port,i));

        }

        persistentStorage = new PersistentStorage(myId);
        log = new Log(myId);
        Console.WriteLine("my id"+myId);
        matchIndex = new int[followers.Count];
        lastHeartbeat = stopwatch.ElapsedMilliseconds;
    }

    public async Task DoAppendEntry(TcpClient connection, AppendEntries packet, CancellationToken ct)
    {
        Console.WriteLine("heartbeat recieved");
        lastHeartbeat = stopwatch.ElapsedMilliseconds;
        currentLeaderId = packet.leaderId; 
        bool success;
        if (packet.term < persistentStorage.CurrentTerm)
        {
            success = false;
        }
        else
        {
            await persistentStorage.SetTermMax(packet.term,ct);
            success = await log.TryAppend(packet.entries,packet.term,packet.prevLogIndex,packet.prevLogTerm);
        }

        if (success)
        {
            //CONSIDER THE LOCK APPLIED ON COMMITS
            //CONSIDER WHAT HAPPENS IN A CHANGE OF LEADERSHIP
            await clientsWaitingLock.WaitAsync(100);
            try
            {
                await ApplyCommits(packet.leaderCommit);
            }
            finally
            {
                clientsWaitingLock.Release();
            }
            
        }
    
        AppendResponse response = new AppendResponse(persistentStorage.CurrentTerm, success);
        await connection.GetStream().WriteAsync(response.Serialize<PacketUnion>(),ct);
    }
    
    
    public async Task RunElection(CancellationToken ct)
    {
        await persistentStorage.IncrementTerm(ct);
        await persistentStorage.SetVotedFor(myId, ct);
        int id = -1;
    
        //Sends out vote requests to all peers
        List<Task> write_tasks = new List<Task>();
        foreach(var peer in followers)
        {
            id++;
            if (id==myId)
            {
                continue;
            }

            int lastLogIndex = log.Length;
            VoteRequest request = new VoteRequest(persistentStorage.CurrentTerm, myId,lastLogIndex,log[lastLogIndex].term);
            var task =Task.Run(()=> peer.AsyncWrite(request.Serialize<PacketUnion>(),50,ct),ct);
            write_tasks.Add(task);
        }

        Console.WriteLine("waiting for votes");
        await Task.WhenAll(write_tasks);
        Console.WriteLine("finished waiting for votes");
        //Wait for votes

        List<Task<PacketUnion?>> ballot_tasks = new List<Task<PacketUnion?>>();
        id = -1;
        foreach (var peer in followers)
        {
            id++;
            if (id==myId)
            {
                continue;
            }

            var task = Task.Run(() => peer.AsyncReadPacket<PacketUnion>(ct,50),ct);
            ballot_tasks.Add(task);
        }

        Console.WriteLine("waiting for ballots"); 
        var ballots = await Task.WhenAll(ballot_tasks);
        Console.WriteLine("finished for ballots"); 
        int vote_count = 1;
        id = -1;
        //Votes have been received. Time to count
        foreach (var  packet in ballots)
        {
            id++;
            if (packet is Ballot ballot)
            {
                if (ballot.term > persistentStorage.CurrentTerm)
                {
                     await persistentStorage.SetTermMax(ballot.term,ct);
                }
                if (ballot.voteGranted)
                {
                    vote_count++;
                }
            }

            if (packet is AppendEntries appendEntries)
            {
                Console.WriteLine("this should never happen");
                return;
            }
        }

        if (vote_count > followers.Count / 2)
        {
            //Congrats we won the vote
            changeMode(RaftMode.Leader,ct,myId);
            return;

        }

        Console.WriteLine("lost the vote: "+vote_count);
        changeMode(RaftMode.Follower,ct); 
    }

    public async Task LeaderFollowerThread(Follower peer, CancellationToken ct)
    {

            var lastTime = stopwatch.ElapsedMilliseconds;
            while (!ct.IsCancellationRequested)
            {
                bool heartbeatRecved = await SendHeartbeat(peer, ct);
                PacketUnion? packet = null;
                if (heartbeatRecved)
                {
                    packet = await peer.AsyncReadPacket<PacketUnion>(ct,5000);
                }
                Console.WriteLine("recved response "+packet + stopwatch.ElapsedMilliseconds);
                if (packet is not null)
                {
                    switch (packet)
                    {
                        case AppendResponse appendResponse:
                            if (appendResponse.term > persistentStorage.CurrentTerm)
                            {
                                Console.WriteLine("switching cuz higher term");
                                changeMode(RaftMode.Follower, ct, null, appendResponse.term);
                                return;
                            }

                            if (appendResponse.success)
                            {
                                nextIndex[peer.id] = log.Length + 1;
                                matchIndex[peer.id] = log.Length;
                                Task.Run(RespondToWaitingUsers).ContinueWith(Util.ReportError);
                            }
                            else
                            {
                                nextIndex[peer.id]--;
                                Console.WriteLine("decrement");
                                continue;
                            }

                            break;
                        case Ballot ballot:
                            Console.WriteLine("weird got ballot even though election is done");
                            if (ballot.term > persistentStorage.CurrentTerm)
                            {
                                changeMode(RaftMode.Follower, ct, null, ballot.term);
                                return;
                            }

                            break;
                        default:
                            Console.WriteLine("THROWN ARUGMENT OUT OF RANGE EXCEPTION");
                            throw new ArgumentOutOfRangeException(nameof(packet));
                    }
                }

                await Task.Delay(HEARTBEATMS, ct);
            }

            Console.WriteLine("normal exit");


    }

    private async Task ApplyCommits(int median)
    {
        int oldCommitIndex = commitIndex;
        commitIndex = int.Max(commitIndex,median);
        for (int idx = oldCommitIndex + 1; idx < commitIndex + 1; idx++)
        {
            commitFunc(log[idx].operation);

        }
        
    }
    private async Task RespondToWaitingUsers()
    {
        await clientsWaitingLock.WaitAsync(100);
        List<(TcpClient, int)> clonedClients;
        List<Task> tasks = new List<Task>();
        List<int> to_remove = new List<int>();
        try
        {

            clonedClients = new List<(TcpClient, int)>(clientsWaiting);
            int median = MedianMatchIndex();
            await ApplyCommits(median);
             
            int i = -1;
            foreach ((TcpClient client, int mindex) in clonedClients)
            {
                i++;
                Console.WriteLine("client waiting: "+ client+" "+mindex);
                if (mindex < median)
                {
                    continue;
                }
                to_remove.Add(i);

                var message = new UserFinalResponse(true, null,log[mindex]);
                ValueTask sendMessageTask = client.GetStream().WriteAsync(message.Serialize<UserResponse>());
                tasks.Add(Task.Run(()=>sendMessageTask.AsTask()).ContinueWith(task=>
                {
                    
                    Util.ReportError(task);
                    client.Dispose();
                }));
            }
        
        }
        finally
        {
            to_remove.Sort();
            to_remove.Reverse();
            foreach (var idx in to_remove)
            {
                clientsWaiting.RemoveAt(idx);
            }
            clientsWaitingLock.Release();
        }
        await Task.WhenAll(tasks);
        
        
    }
    public async Task LeaderHandleClient(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            while (!ct.IsCancellationRequested)
            {
                PacketUnion? packet = await Util.Deserialize<PacketUnion>(client.GetStream(),-1, ct);
                if (packet is null)
                {
                    return;
                }
                switch (packet)
                {
                    case AppendEntries appendEntries:
                        Console.WriteLine("GOT HEARTBEAT");
                        if (appendEntries.term > persistentStorage.CurrentTerm)
                        {
                            if (changeMode(RaftMode.Follower, ct, appendEntries.leaderId, appendEntries.term))
                            {
                                Task.Run(() => DoAppendEntry(client, appendEntries, ct), ct).ContinueWith(Util.ReportError);
                            }
                        }
                        else if(appendEntries.leaderId == currentLeaderId || currentLeaderId == null)
                        {
                            Task.Run(() => DoAppendEntry(client, appendEntries, ct), ct).ContinueWith(Util.ReportError);
                        }
                        else
                        {
                            Console.WriteLine("someone tried to be a leader");
                        }

                        break;
                    case VoteRequest voteRequest:
                        Console.WriteLine("GOT VOTE REQUEST");

                        //MUTEX
                        await persistentStorage.SetTermMax(voteRequest.term,ct);

                        bool voteYes = voteRequest.lastLogTerm > log[log.Length].term;
                        voteYes |= voteRequest.lastLogTerm == log[log.Length].term &&
                                   voteRequest.lastLogIndex >= log.Length;
                        voteYes |=  persistentStorage.VotedFor is null;
                        if (voteYes)
                        {
                            if (changeMode(RaftMode.Follower, ct, voteRequest.candidateId, voteRequest.term))
                            {
                                Ballot b = new Ballot(persistentStorage.CurrentTerm, true);
                                Console.WriteLine("voted true");
                                await client.GetStream().WriteAsync(b.Serialize<PacketUnion>());
                            }
                        }
                        else
                        {
                            Ballot b = new Ballot(persistentStorage.CurrentTerm, false);
                            Console.WriteLine("voted false");
                            await client.GetStream().WriteAsync(b.Serialize<PacketUnion>(), ct);
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(packet));
                }
                Console.WriteLine(log);
            }
        }

    }
    public async Task ClientListen(CancellationToken ct,bool doTimeout)
    {
        TcpListener listener = new TcpListener(server_ip);
        listener.Start();
        Console.WriteLine("start lsiten");
        using (listener)
        {

            if (doTimeout)
            {
                var timeoutTask = Task.Run(async () =>
                {
                    var randWait = Random.Shared.Next(0, 1000);
                    Console.WriteLine("RAN WAIT: "+randWait);
                    var now = stopwatch.ElapsedMilliseconds;
                    while (!ct.IsCancellationRequested)
                    {
                        var waitTime = HEARTBEATMS*2 - now + lastHeartbeat + randWait;
                        if (waitTime < 0)
                        {
                            if (changeMode(RaftMode.RunningElection, ct))
                            {
                                return;
                            }
                        }

                        await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
                        now = stopwatch.ElapsedMilliseconds;
                    }
                },ct).ContinueWith(task =>
                {
                    Console.WriteLine("closing timouttask");
                });
            }

            var tasks = new List<Task>();
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    TcpClient client = await listener.AcceptTcpClientAsync(ct);
                    Console.WriteLine("got client: "+client.Client.RemoteEndPoint);
                    tasks.Add(Task.Run(() => LeaderHandleClient(client, ct), ct).ContinueWith(Util.ReportError));
                }
            }
            catch (OperationCanceledException)
            {
            }

            await Task.WhenAll(tasks);
            

            throw new OperationCanceledException();
        }

    }
    public async Task LeaderLoop(CancellationToken ct)
    {
        Debug.Assert(mode==RaftMode.Leader);
        List<Task> tasks_list = new List<Task>();
        foreach (var follower in followers)
        {
            if (follower is null)
            {
                continue;
            }
            var task = Task.Run(()=>LeaderFollowerThread(follower,ct),ct).ContinueWith(Util.ReportError);
            tasks_list.Add(task); 
        }



        await Task.WhenAll(tasks_list);
        Debug.Assert(mode == RaftMode.Follower);
    }
    public async Task<bool> SendHeartbeat(Follower peer, CancellationToken ct)
    {
        Debug.Assert(peer.id != myId);
        var idx = nextIndex[peer.id] - 1;
        try
        {
            var operations = await log.OperationsAt(nextIndex[peer.id]);
            AppendEntries appendEntries = new AppendEntries(persistentStorage.CurrentTerm, myId, idx, log[idx].term, operations,commitIndex);
            await peer.AsyncWrite(appendEntries.Serialize<PacketUnion>(),500, ct);
            Console.WriteLine("heart beat sent to "+peer.hostname+":"+peer.port+" "+stopwatch.ElapsedMilliseconds);
            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine("CAUGHT EXCPETION: "+e);
        }

        return false;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing && !disposed)
        {
            followers.ForEach(n => n.Dispose());
            disposed = true;
        } 
    }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~RaftServer()
    {
        Dispose(false);
    }
    public async Task Run()
    {
        
        mode = RaftMode.Follower;
        Task.Run(ListenForUser).ContinueWith(Util.ReportError);
        while (true)
        {
            Task listen_task = null;
            
            try
            {
                var b = raft_cts.Token;
            }
            catch
            {
                Console.WriteLine("ASDASDASDHASDFHASDFHLLASDDFJKASHJFHSDAJKFHSDJKAHASD");
            }
                
            CancellationToken ct = raft_cts.Token;
            try
            {
                switch (mode)
                {
                    case RaftMode.RunningElection:
                        listen_task = Task.Run(() => ClientListen(ct, false), ct);
                        await RunElection(ct);
                        break;
                    case RaftMode.Leader:
                        listen_task = Task.Run(() => ClientListen(ct, false), ct);
                        await LeaderLoop(ct);
                        break;
                    case RaftMode.Follower:
                        await ClientListen(ct, true);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (OperationCanceledException)
            {
            }
            try
            {
                if (listen_task is not null)
                {
                    await listen_task.WaitAsync(CancellationToken.None);
                }
            }
            catch(OperationCanceledException){}
            old_cts.Dispose();
        }
    }

    private int MedianMatchIndex()
    {
        
        var ordered = matchIndex.Order().ToArray();
        return ordered[ordered.Length / 2+1];
    }
    private async Task HandleUser(TcpClient client)
    {
        
        DoOperation? deserialized =
            await Util.Deserialize<DoOperation>(client.GetStream(), 100, CancellationToken.None);
        if (deserialized is null)
        {
            Console.WriteLine("Failed to parse package");
            return;
        }
        if (currentLeaderId != myId)
        {
            var response = new UserFinalResponse(false, server_ip.ToString(),null);
            await client.GetStream().WriteAsync(response.Serialize<UserResponse>());
            return;
        }

        if (currentLeaderId is null)
        {
            var response = new UserFinalResponse(false, null,null);
            await client.GetStream().WriteAsync(response.Serialize<UserResponse>());
            return;
            
        }

        Operation op = deserialized.op;
        
        Console.WriteLine("recieved a user message "+op);
        await clientsWaitingLock.WaitAsync(100);
        try
        {
            int mindex = await log.LeaderAdd(op,persistentStorage.CurrentTerm);
            clientsWaiting.Add((client,mindex));
        }
        finally
        {
            clientsWaitingLock.Release();
        }



    }
    private async Task ListenForUser()
    {
        TcpListener listener = new TcpListener(user_server_ip);
        listener.Start();
        while (true)
        {
            TcpClient client = await listener.AcceptTcpClientAsync();
            Task.Run(() => HandleUser(client)).ContinueWith(task=>
            {
                Util.ReportError(task);
            });

        }

    }
    public static void Main()
    {
        var serverport = Environment.GetEnvironmentVariable("server-port");
        int parsed_server_port = 0;
        if (!int.TryParse(serverport, out parsed_server_port))
        {
            Console.WriteLine("failed to parse/read server port (maybe add 'server-port' env variable). Defaulting to 8080");
            parsed_server_port = 8080;
        }
        var userport = Environment.GetEnvironmentVariable("user-port");
        
        int parsed_client_port = 0;
        if (!int.TryParse(userport, out parsed_client_port))
        {
            Console.WriteLine("failed to parse/read user port (maybe add 'user-port' env variable). Defaulting to 8000");
            parsed_client_port = 8000;
        }

        Console.WriteLine("starting server on server port "+parsed_server_port);
        Console.WriteLine("starting server on user port "+parsed_client_port);
        // Console.WriteLine("booting up");
        // int port = Convert.ToInt32(Console.ReadLine());
        var raftServer = new RaftServer(parsed_server_port,parsed_client_port, async(op) => 
        {
            Console.WriteLine("commited "+op);
        });
        // // raftServer.server_ip = new IPEndPoint(0, 0);
        try
        {
            raftServer.Run().Wait();

        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }

        Console.WriteLine("HOLDING");
        while (true)
        {
            
        }
        
        // Console.ReadLine();
    }
}