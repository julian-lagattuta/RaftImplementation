using MessagePack;

namespace postgretest;
[MessagePack.Union(0,typeof(Write))]
[MessagePack.Union(1,typeof(Get))]
[MessagePack.Union(2,typeof(Delete))]
[MessagePack.Union(3,typeof(NOP))]
public abstract record Operation;

[MessagePackObject]
public sealed record Write : Operation
{
    [Key(0)]
    public string key;
    [Key(1)]
    public string value;
    public Write(string _key, string _value)
    {
        key = _key;
        value = _value;

    }
    public void Deconstruct(out string _key,out string _value)
    {
        _key = key;
        _value = value;
    }
}
[MessagePackObject]
public sealed record Get: Operation
{
    [Key(0)]
    public string key;
    public Get(string _key)
    {
        key = _key;
    }
    public void Deconstruct(out string _key)
    {
        _key = key;
    }
}
[MessagePackObject]
public sealed record Delete: Operation
{
    [Key(0)]
    public string key;
    public Delete(string _key)
    {
        key = _key;
    }

    public void Deconstruct(out string _key)
    {
        _key = key;
    }
}

[MessagePackObject]
public sealed record NOP : Operation;
