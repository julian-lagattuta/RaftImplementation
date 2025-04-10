using MessagePack;

namespace postgretest;
[MessagePackObject(true)]
record PersistentData(
    int currentTerm,
    int? votedFor
);
public class PersistentStorage: IDisposable
{
    private volatile int _currentTerm= 0;
    private int? _votedFor = null;
    private int id = 0;
    public int? VotedFor
    {
        get
        {
            return _votedFor;
        }
    }
    public int CurrentTerm
    {
        get
        {
            
            return _currentTerm;
        }
    }
    
    private string persistentFilePath;
    private SemaphoreSlim storage_lock = new SemaphoreSlim(1,1);

    // public async Task<byte[]> Serialize(PersistentData data,CancellationToken ct)
    // {
    //     MessagePackSerializer.SerializeAsync(data, ct);
    // }
    public async Task IncrementTerm(CancellationToken ct)
    {
        await storage_lock.WaitAsync(ct);
        try
        {
            ct.ThrowIfCancellationRequested();
            PersistentData data = new PersistentData(_currentTerm+1, _votedFor);
            byte[] buffer = new byte[256];
            using (MemoryStream stream = new MemoryStream(buffer))
            {
                await MessagePackSerializer.SerializeAsync<PersistentData>(stream, data, null, ct);
            }

            await File.WriteAllBytesAsync(persistentFilePath, buffer, ct);
            _currentTerm++;
        }
        finally
        {
            storage_lock.Release();
        }
    }
    public async Task SetTermMax(int newTerm, CancellationToken ct)
    {
        await storage_lock.WaitAsync(ct);
        try
        {
            ct.ThrowIfCancellationRequested();
            PersistentData data = new PersistentData(newTerm, _votedFor);
            byte[] buffer = new byte[256];
            using (MemoryStream stream = new MemoryStream(buffer))
            {
                await MessagePackSerializer.SerializeAsync<PersistentData>(stream, data, null, ct);
            }

            await File.WriteAllBytesAsync(persistentFilePath, buffer, ct);
            _currentTerm = int.Max(newTerm, _currentTerm);
        }
        finally
        {
            storage_lock.Release();
        }
    }
    public async Task SetVotedFor(int? votedFor, CancellationToken ct)
    {
        await storage_lock.WaitAsync(ct);
        try{
            ct.ThrowIfCancellationRequested();
            PersistentData data = new PersistentData(_currentTerm, votedFor);
            byte[] buffer = new byte[256];
            MemoryStream stream = new MemoryStream(buffer);
            await MessagePackSerializer.SerializeAsync<PersistentData>(stream, data, null, ct);
            await File.WriteAllBytesAsync(persistentFilePath, buffer, ct);
            _votedFor = votedFor;
        }
        finally
        {
            storage_lock.Release();
        }
        
    }
    public PersistentStorage(int _id)
    {
        id = _id;
        if (!Directory.Exists(Util.AppFolder))
        {
            throw new Exception("not found path " + Util.AppFolder);
        }

        persistentFilePath = Path.Combine(Util.AppFolder, "persistentFile"+id);
        if (File.Exists(persistentFilePath))
        {
            byte[] data = File.ReadAllBytes(persistentFilePath);
            PersistentData deserialized = MessagePackSerializer.Deserialize<PersistentData>(data);
            _votedFor = deserialized.votedFor;
            _currentTerm = deserialized.currentTerm;
        }
        
    }

    public void Dispose()
    {
        storage_lock.Dispose();
    }
}