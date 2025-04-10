using MessagePack;

namespace postgretest;

[MessagePackObject(true)]
public record LogEntry(
    int term,
    int index,
    Operation operation
);