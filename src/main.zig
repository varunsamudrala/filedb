const std = @import("std");
const datafile = @import("./datafile.zig");
const utils = @import("utils.zig");
const oldfiles = @import("oldfiles.zig");
const kd = @import("keydir.zig");
const Record = @import("record.zig").Record;
// open file
// mutex
// bufPool
// keydir
// old files
// lock files

const FileDB = struct {
    datafile: datafile.Datafile,
    bufPool: std.heap.MemoryPool([]const u8),
    keydir: std.AutoHashMap(u64, kd.metadata),
    oldfiles: oldfiles.OldFiles,
    mu: std.Thread.Mutex,
    allocator: std.mem.Allocator,

    pub fn init(id: u32, allocator: std.mem.Allocator) !*FileDB {
        const filedb = try allocator.create(FileDB);
        filedb.* = .{
            .datafile = try datafile.Datafile.init(id),
            .keydir = std.AutoHashMap(u64, kd.metadata).init(allocator),
            .oldfiles = try oldfiles.OldFiles.init(allocator),
            .bufPool = std.heap.MemoryPool([]const u8).init(allocator),
            .mu = std.Thread.Mutex{},
            .allocator = allocator,
        };
        return filedb;
    }

    pub fn deinit(self: *FileDB, allocator: std.mem.Allocator) void {
        self.oldfiles.deinit();
        self.datafile.deinit();
        self.keydir.deinit();
        allocator.destroy(self);
    }

    pub fn put(self: *FileDB, key: []const u8, value: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try utils.validateKV(key, value);
        try self.storeKV(key, value);
    }

    fn storeKV(self: *FileDB, key: []const u8, value: []const u8) !void {
        const record = try Record.init(self.allocator, key, value);
        defer record.deinit(self.allocator);

        const metadata = kd.metadata.init(self.datafile.id, key.len, value.len, record.tstamp);

        const record_size = @sizeOf(Record) - @sizeOf([]u8) * 2 + record.key_len + record.value_len;
        const buf = try self.allocator.alloc(u8, record_size);
        defer self.allocator.free(buf); // Free buffer after writing
        try record.encode(buf);

        std.debug.print("Record: {}", .{record});
        std.debug.print("metadata: {}", .{metadata});
        std.debug.print("encoded: {d}", .{buf});
    }
};
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    const id = 2;
    const filedb = try FileDB.init(id, allocator);
    defer filedb.deinit(allocator);
    try filedb.put("hello", "cow");
}
