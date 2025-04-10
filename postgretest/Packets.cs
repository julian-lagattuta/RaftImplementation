using System.Diagnostics;
using MessagePack;

namespace postgretest;

public abstract record Packet  
{
    public byte[] Serialize<T>() where T: Packet
    {
        
        byte[] arr = MessagePackSerializer.Serialize((T)this);

        byte[] size = BitConverter.GetBytes(arr.Length);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(size);
        }

        return size.Concat(arr).ToArray();
    }
}
[MessagePack.Union(0, typeof(VoteRequest))]
[MessagePack.Union(1, typeof(AppendEntries))]
[MessagePack.Union(2, typeof(Ballot))]
[MessagePack.Union(3, typeof(AppendResponse))]
public abstract record PacketUnion: Packet
{

}
[MessagePackObject(true)]public record VoteRequest(
    int term,
    int candidateId,
    int lastLogIndex,
    int lastLogTerm
) : PacketUnion;

[MessagePackObject(true)]public record AppendEntries(
    int term,
    int leaderId,
    int prevLogIndex, 
    int prevLogTerm,
    LogEntry[] entries,
    int leaderCommit
) : PacketUnion;

[MessagePackObject(true)]
public record Ballot(
    int term,
    bool voteGranted
) : PacketUnion;
[MessagePackObject(true)]
public record AppendResponse(
    int term,
    bool  success
) : PacketUnion;



[MessagePackObject(true)]
public record DoOperation(
    Operation op
) : Packet;

[MessagePack.Union(0,typeof(UserFinalResponse))]
public abstract record UserResponse: Packet;
[MessagePackObject(true)]
public record UserFinalResponse(
    bool success,
    string leaderIP,
    LogEntry? entry
): UserResponse;
[MessagePackObject(true)]
public record UserImmediateResponse(
    LogEntry entry
): UserResponse;
