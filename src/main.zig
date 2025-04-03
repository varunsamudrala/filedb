const std = @import("std");
const datafile = @import("./datafile.zig");
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
    bufPool: std.heap.MemoryPool([]const u8),
    keydir: std.StringHashMap(std.ArrayList(kd.metadata)),
    oldfiles: *oldfiles.OldFiles,
    mu: std.Thread.Mutex,
    allocator: std.mem.Allocator,

    pub fn init(id: u32, allocator: std.mem.Allocator) !*FileDB {
        const filedb = try allocator.create(FileDB);
        filedb.* = .{
            .datafile = try datafile.Datafile.init(id),
            .keydir = std.StringHashMap(std.ArrayList(kd.metadata)).init(allocator),
            .oldfiles = try oldfiles.OldFiles.init(allocator),
            .bufPool = std.heap.MemoryPool([]const u8).init(allocator),
            .mu = std.Thread.Mutex{},
            .allocator = allocator,
        };
        return filedb;
    }

    pub fn deinit(self: *FileDB, allocator: std.mem.Allocator) void {
        self.oldfiles.deinit(allocator);
        self.datafile.deinit();
        var keydirIterator = self.keydir.valueIterator();
        while (keydirIterator.next()) |entry| {
            entry.deinit();
        }
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
        const record = try Record.Record.init(self.allocator, key, value);
        defer record.deinit(); // free record after writingo

        const record_size = @sizeOf(Record.Record) - @sizeOf([]u8) * 2 + record.key_len + record.value_len;
        const buf = try self.allocator.alloc(u8, record_size);
        defer self.allocator.free(buf); // Free buffer after writing

        try record.encode(buf);

        //store to datafile
        const offset = try self.datafile.store(buf);
        //store to keydir
        const metadata = kd.metadata.init(self.datafile.id, record_size, offset, record.tstamp);
        var entry = try self.keydir.getOrPut(key);
        if (!entry.found_existing) {
            entry.value_ptr.* = std.ArrayList(kd.metadata).init(self.allocator);
        }
        try entry.value_ptr.append(metadata);

        //fsync optional
    }

    pub fn get(self: *FileDB, key: []const u8) !?std.ArrayList([]const u8) {
        self.mu.lock();
        defer self.mu.unlock();

        const result = self.getValue(key);
        return result;
    }

    fn getValue(self: *FileDB, key: []const u8) !?std.ArrayList([]const u8) {
        // for (self.keydir.iterator())
        const metadata = self.keydir.get(key);
        if (metadata == null) {
            // if no key exists in the keydir
            return undefined;
        }
        var responseArray = std.ArrayList([]const u8).init(self.allocator);
        errdefer responseArray.deinit(); // Free list on error

        for (metadata.?.items) |entry| {
            const buf = try self.allocator.alloc(u8, entry.value_sz);
            defer self.allocator.free(buf);
            // defer self.allocator.destroy(buf);
            if (self.datafile.id == entry.file_id) {
                // get data from datafile
                try self.datafile.get(buf, entry.value_pos, entry.value_sz);
            } else {
                //get data from oldfiles
                try self.oldfiles.get(buf, entry.file_id, entry.value_pos, entry.value_sz);
            }
            //decode data and put in response array
            const record = try Record.decodeRecord(self.allocator, buf);
            defer record.deinit();

            const value = try self.allocator.dupe(u8, record.value);
            try responseArray.append(value);
        }
        return responseArray;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    // initialize id of new file
    const id = 2;

    const filedb = try FileDB.init(id, allocator);
    defer filedb.deinit(allocator);
    try filedb.put("hello", "cow");
    std.log.debug("\n", .{});
    try filedb.put("hello", "nakndeaekacow");
    std.log.debug("\n", .{});
    try filedb.put("knsknrknknhello", "audfjendjsd");

    const value = try filedb.get("hello");
    std.log.debug("found value {any}", .{value.?.items});
    for (value.?.items) |buf| {
        allocator.free(buf); // Free each allocated buffer
    }
    value.?.deinit(); // Deinitialize the ArrayList itself
}
