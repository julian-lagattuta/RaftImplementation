namespace postgretest;

public class Database
{
    private Dictionary<string, string> data = new Dictionary<string, string>();

    public void Write(string key, string value)
    {
        data[key] = value;
    }
    public void Delete(string key)
    {
        data.Remove(key);
    }

    public void PerformOperation(Operation operation)
    {
        if (operation is Write(string key1, var value))
        {
            Write(key1, value);
        }
        else if (operation is Delete(string key2))
        {
            Delete(key2);
        }
    }
    public string? Get(string key)
    {
        if (data.ContainsKey(key))
        {
            return data[key];
        }

        return null;


    }
}