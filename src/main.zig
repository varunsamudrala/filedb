const std = @import("std");
const datafile = @import("./datafile.zig");
// open file
// reader, writer
// mutex
// bufPool
// keydir
// old files
// lock files

const FileDB = struct {};
pub fn main() !void {
    const data = try datafile.Datafile.init(1);
    std.debug.print("Datafile 1 - ID: {}, Offset: {}\n", .{ data.id, data.offset });
}
