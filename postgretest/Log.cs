using System.Diagnostics;
using System.Text.Json.Serialization;
using MessagePack;
using Newtonsoft.Json;
using JsonConverter = Newtonsoft.Json.JsonConverter;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace postgretest;

public class Log
{
    private List<LogEntry> log = new List<LogEntry>();
    private SemaphoreSlim logLock = new SemaphoreSlim(1,1);
    private SemaphoreSlim fileLock = new SemaphoreSlim(1,1);
    private string logFilename; 
    public Log(int _id)
    {
        logLock.Wait(1000);
        log.Add(new LogEntry(-1,log.Count+1,new NOP()));
        logLock.Release();
        logFilename = Path.Combine(Util.AppFolder,"log"+_id);
        try
        {

            if (Path.Exists(logFilename))
            {
                var data = File.ReadAllBytes(logFilename);
                log = MessagePackSerializer.Deserialize<List<LogEntry>>(data);
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("failed to parse log "+e);
        }

    }

    private async Task UpdateEntries()
    {
        await fileLock.WaitAsync(1000);
        try
        {
            var data =MessagePackSerializer.Serialize(log);
            await File.WriteAllBytesAsync(logFilename, data);
        }
        finally
        {
            fileLock.Release();
        }
    }

    public override string ToString()
    {
        return string.Join(", ",log);
    }

    public LogEntry this[int key]
    {
        get
        {
            logLock.Wait(1000);
            var v = log[key];
            logLock.Release();
            return v;
        }
        set
        {
            logLock.Wait(1000);
            log[key] = value;
            logLock.Release();
        }
    }

    public int Count
    {
        get
        {
            logLock.Wait(1000);
            var v = log.Count - 1;
            logLock.Release();
            return v;
        }
    }

    public int Length
    {
        get => Count;
    }

    public async Task<int> LeaderAdd(Operation op, int term)
    {
        await logLock.WaitAsync(1000);
        try
        {
            log.Add(new LogEntry(term,log.Count,op));
            Task.Run(UpdateEntries);
            return log.Count-1;
        }
        finally
        {
            logLock.Release();
        }
    }
    public async Task<bool> TryAppend(LogEntry[] operations,int currentTerm, int prevLogIndex,int prevLogTerm)
    {
        
        await logLock.WaitAsync(1000);
        try
        {
            if (log.Count - 1 < prevLogIndex)
            {
                return false;
            }

            Console.WriteLine("prevlongindex " + prevLogIndex);
            Console.WriteLine(log.Count);
            if (log[prevLogIndex].term != prevLogTerm)
            {
                return false;
            }

            if (operations.Length == 0)
            {
                return true;
            }

            Console.WriteLine("prevlongindex " + prevLogIndex);
            if (prevLogIndex + 1 < log.Count)
            {
                log.RemoveRange(prevLogIndex + 1, log.Count - prevLogIndex - 1);
            }

            Console.WriteLine("adding range: " + operations.Length);
            log.AddRange(operations);
            if (operations.Length > 0)
            {
                await UpdateEntries();
            }

            return true;

        }
        finally
        {
            logLock.Release();
        }
        
    }

    public async Task<LogEntry[]> OperationsAt(int nextIndex)
    {
        if (nextIndex > Length)
        {
            return [];
        }
       await logLock.WaitAsync(1000);
       Console.WriteLine("next: "+nextIndex);
       Console.WriteLine("count: "+log.Count);
       var v = log.GetRange(nextIndex, log.Count-nextIndex).ToArray();
       logLock.Release();
       return v;
    }
}