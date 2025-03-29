const std = @import("std");
const datafile = @import("./datafile.zig");
const utils = @import("utils.zig");
const oldfiles = @import("oldfiles.zig");
const kd = @import("keydir.zig");
// open file
// mutex
// bufPool
// keydir
// old files
// lock files

const FileDB = struct {
    datafile: datafile.Datafile,
    bufPool: std.heap.MemoryPool([]const u32),
    keydir: std.AutoHashMap(u64, kd.metadata),
    oldfiles: oldfiles.OldFiles,

    pub fn init(id: u32, allocator: std.mem.Allocator) !*FileDB {
        const filedb = try allocator.create(FileDB);
        filedb.* = .{
            .datafile = try datafile.Datafile.init(id),
            .keydir = std.AutoHashMap(u64, kd.metadata).init(allocator),
            .oldfiles = try oldfiles.OldFiles.init(allocator),
            .bufPool = std.heap.MemoryPool([]const u32).init(allocator),
        };
        return filedb;
    }

    pub fn deinit(self: *FileDB, allocator: std.mem.Allocator) void {
        self.oldfiles.deinit();
        self.datafile.deinit();
        self.keydir.deinit();
        allocator.destroy(self);
    }
};
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    const id = 2;
    const filedb = try FileDB.init(id, allocator);
    defer filedb.deinit(allocator);
}
