const std = @import("std");
const datafile = @import("datafile.zig");
const utils = @import("utils.zig");
const oldfiles = @import("oldfiles.zig");
const kd = @import("keydir.zig");
const Record = @import("record.zig");
// open file
// mutex
// bufPool
// keydir
// old files
// lock files

const FileDB = struct {
    datafile: datafile.Datafile,
    keydir: std.StringHashMap(kd.Metadata),
    oldfiles: *oldfiles.OldFiles,
    mu: std.Thread.Mutex,
    allocator: std.mem.Allocator,

    // initialize the filedb with keydir and other structures
    pub fn init(allocator: std.mem.Allocator) !*FileDB {
        const keydir = try kd.loadKeyDir(allocator);
        const oldfile = try oldfiles.OldFiles.init(allocator);

        // get the last used fileid
        var id: u32 = 1;
        var it = oldfile.filemap.keyIterator();
        while (it.next()) |entry| {
            if (entry.* >= id) {
                id = entry.* + 1;
            }
        }

        const filedb = try allocator.create(FileDB);
        filedb.* = .{
            .datafile = try datafile.Datafile.init(id),
            .keydir = keydir,
            .oldfiles = oldfile,
            .mu = std.Thread.Mutex{},
            .allocator = allocator,
        };
        return filedb;
    }

    pub fn deinit(self: *FileDB) void {
        var key_it = self.keydir.iterator();
        while (key_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }

        self.keydir.deinit();

        // Clean up other resources
        self.oldfiles.deinit();
        self.datafile.deinit();

        // Finally free the FileDB struct itself
        self.allocator.destroy(self);
    }

    pub fn put(self: *FileDB, key: []const u8, value: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try utils.validateKV(key, value);
        try self.storeKV(key, value);
    }

    fn storeKV(self: *FileDB, key: []const u8, value: []const u8) !void {
        const record = try Record.Record.init(self.allocator, key, value);
        defer record.deinit();

        const record_size = @sizeOf(Record.Record) - @sizeOf([]u8) * 2 + record.key_len + record.value_len;
        const buf = try self.allocator.alloc(u8, record_size);
        defer self.allocator.free(buf);

        try record.encode(buf);
        const offset = try self.datafile.store(buf);
        const metadata = kd.Metadata.init(self.datafile.id, record_size, offset, record.tstamp);

        // get the entry if already present, if doesnt exist, dynamically allocate a new
        // key else reuse the old one.
        const entry = try self.keydir.getOrPut(key);
        if (!entry.found_existing) {
            const copy_key = try self.allocator.dupe(u8, key);
            entry.key_ptr.* = copy_key;
        }
        entry.value_ptr.* = metadata;

        //possible fsync
    }

    pub fn get(self: *FileDB, key: []const u8) !?[]const u8 {
        self.mu.lock();
        defer self.mu.unlock();

        const result = self.getValue(key);
        return result;
    }

    fn getValue(self: *FileDB, key: []const u8) !?[]const u8 {
        const metadata = self.keydir.get(key);
        if (metadata == null) {
            // if no key exists in the keydir
            return undefined;
        }

        const buf = try self.allocator.alloc(u8, metadata.?.value_sz);
        defer self.allocator.free(buf);
        // defer self.allocator.destroy(buf);
        if (self.datafile.id == metadata.?.file_id) {
            // get data from datafile
            try self.datafile.get(buf, metadata.?.value_pos, metadata.?.value_sz);
        } else {
            //get data from oldfiles
            try self.oldfiles.get(buf, metadata.?.file_id, metadata.?.value_pos, metadata.?.value_sz);
        }
        //decode data and put in response array
        const record = try Record.decodeRecord(self.allocator, buf);
        defer record.deinit();

        const value = try self.allocator.dupe(u8, record.value);
        return value;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    const filedb = try FileDB.init(allocator);
    defer filedb.deinit();
    var it = filedb.keydir.iterator();
    while (it.next()) |entry| {
        std.log.debug("key:{s} value: {}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
    try filedb.put("a", "value1");
    try filedb.put("b", "value2");
    try filedb.put("f", "value2");
    try filedb.put("g", "value2");
    try filedb.put("c", "value3");
    try filedb.put("d", "value4");
    try filedb.put("e", "value5");
    try filedb.put("a", "large_value");
    try filedb.put("b", "extra_large_value");
    try filedb.put("c", "sm12");

    it = filedb.keydir.iterator();
    while (it.next()) |entry| {
        std.log.debug("key:{s} value: {}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
    defer kd.storeHashMap(&filedb.keydir) catch |err| {
        std.log.debug("Error storing hashmap: {}", .{err});
    };
    const value = try filedb.get("c");
    if (value == null) {
        std.log.debug("Value Not found in DB", .{});
        return;
    }
    defer allocator.free(value.?);
    const final_value = value.?;
    std.log.debug("found value '{s}'", .{final_value});
}
