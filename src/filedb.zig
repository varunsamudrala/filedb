const std = @import("std");
const time = std.time;
const expect = std.testing.expect;
const Datafile = @import("datafile.zig").Datafile;
const utils = @import("utils.zig");
const Oldfiles = @import("oldfiles.zig").OldFiles;
const kd = @import("keydir.zig");
const record = @import("record.zig");
const config = @import("config.zig");
const Logger = @import("logger.zig").Logger;

pub const FileDB = struct {
    datafile: *Datafile,
    keydir: std.StringHashMap(kd.Metadata),
    oldfiles: *Oldfiles,
    mu: std.Thread.Mutex,
    allocator: std.mem.Allocator,
    config: config.Options,
    log: Logger,
    compaction_thread: ?std.Thread = null,
    dfRotation_thread: ?std.Thread = null,
    sync_thread: ?std.Thread = null,
    should_shutdown: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    // initialize the filedb with keydir and other structures
    pub fn init(allocator: std.mem.Allocator, options: ?config.Options) !*FileDB {
        var conf = config.defaultOptions();
        if (options) |option| {
            conf = option;
        }

        // Initialize oldfiles
        const oldfilesMap = std.AutoHashMap(u32, *Datafile).init(allocator);
        var dir = try utils.openUserDir(conf.dir);
        defer dir.close();
        const oldfiles = try Oldfiles.init(allocator, oldfilesMap);
        try oldfiles.initializeMap(dir);
        // Initialize keydir first
        const keydir = std.StringHashMap(kd.Metadata).init(allocator);

        const filedb = try allocator.create(FileDB);
        filedb.* = .{
            .mu = std.Thread.Mutex{},
            .allocator = allocator,
            .config = conf,
            .keydir = keydir,
            .oldfiles = oldfiles,
            .datafile = undefined, // Will be set after calculating the ID
            .log = Logger.init(conf.log_level),
        };
        filedb.log.info("============= STARTING FILEDB ================", .{});
        // get the last used fileid
        var id: u32 = 1;
        var it = filedb.oldfiles.filemap.keyIterator();
        while (it.next()) |entry| {
            if (entry.* >= id) {
                id = entry.* + 1;
            }
        }
        filedb.log.debug("last used file id: {}", .{id});
        filedb.datafile = try Datafile.init(allocator, id, dir);

        // Load keydir data
        try filedb.loadKeyDir();

        filedb.compaction_thread = try std.Thread.spawn(.{}, runCompaction, .{filedb});
        filedb.dfRotation_thread = try std.Thread.spawn(.{}, runRotation, .{filedb});
        filedb.sync_thread = try std.Thread.spawn(.{}, runSync, .{filedb});

        filedb.log.info("================== FileDB created ===============", .{});
        return filedb;
    }

    pub fn deinit(self: *FileDB) void {
        self.should_shutdown.store(true, .release);

        // Join thread if it was started
        if (self.compaction_thread) |t| {
            t.join();
        }
        if (self.dfRotation_thread) |t| {
            t.join();
        }
        if (self.sync_thread) |t| {
            t.join();
        }

        self.storeHashMap() catch |err| {
            self.log.err("Error storing hashmap: {}", .{err});
        };

        var key_it = self.keydir.iterator();
        while (key_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }

        self.keydir.deinit();

        // Clean up other resources
        self.oldfiles.deinit();
        self.datafile.deinit();

        // Finally free the FileDB struct itself
        self.log.info("SHUTTING DOWN FILEDB", .{});
        self.allocator.destroy(self);
    }

    pub fn put(self: *FileDB, key: []const u8, value: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try utils.validateKV(key, value);
        try self.storeKV(self.datafile, key, value);
    }

    pub fn get(self: *FileDB, key: []const u8) !?[]const u8 {
        self.mu.lock();
        defer self.mu.unlock();

        const result = self.getValue(key);
        return result;
    }

    fn storeKV(self: *FileDB, datafile: *Datafile, key: []const u8, value: []const u8) !void {
        const rec = try record.Record.init(self.allocator, key, value);
        defer rec.deinit();

        const record_size = @sizeOf(record.Record) - @sizeOf([]u8) * 2 + rec.key_len + rec.value_len;
        const buf = try self.allocator.alloc(u8, record_size);
        defer self.allocator.free(buf);

        try rec.encode(buf);
        const offset = try datafile.store(buf);
        const metadata = kd.Metadata.init(datafile.id, record_size, offset, rec.tstamp);

        // get the entry if already present, if doesnt exist,
        // dynamically allocate a new key else reuse the old one.
        const entry = try self.keydir.getOrPut(key);
        if (!entry.found_existing) {
            const copy_key = try self.allocator.dupe(u8, key);
            entry.key_ptr.* = copy_key;
        }
        entry.value_ptr.* = metadata;

        //possible fsync
        if (self.config.alwaysFsync) {
            try self.datafile.sync();
        }
    }

    fn getValue(self: *FileDB, key: []const u8) !?[]const u8 {
        const metadata = self.keydir.get(key);
        if (metadata == null) {
            // if no key exists in the keydir
            return undefined;
        }

        const buf = try self.allocator.alloc(u8, metadata.?.value_sz);
        defer self.allocator.free(buf);
        if (self.datafile.id == metadata.?.file_id) {
            // get data from datafile
            try self.datafile.get(buf, metadata.?.value_pos, metadata.?.value_sz);
        } else {
            //get data from oldfiles
            try self.oldfiles.get(buf, metadata.?.file_id, metadata.?.value_pos, metadata.?.value_sz);
        }
        //decode data and put in response array
        const rec = try record.decodeRecord(self.allocator, buf);
        defer rec.deinit();

        const value = try self.allocator.dupe(u8, rec.value);
        return value;
    }

    pub fn delete(self: *FileDB, key: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        // update the value of the key with a tombstone value
        const data = [_]u8{};
        try self.storeKV(self.datafile, key, &data);

        // remove the key from the keydir and deallocate the key memory.
        const entry = self.keydir.fetchRemove(key);
        if (entry) |e| {
            // Free the dynamically allocated key
            self.allocator.free(e.key);
            std.log.info("Deleted key: {s}", .{key});
        } else {
            std.log.info("Key not found: {s}", .{key});
        }
    }

    pub fn list(self: *FileDB, allocator: std.mem.Allocator) !std.ArrayList([]u8) {
        self.mu.lock();
        defer self.mu.unlock();

        // Initialize the array list
        var keylist = std.ArrayList([]u8).init(allocator);
        errdefer {
            // Free any keys we've already added if we encounter an error
            for (keylist.items) |item| {
                allocator.free(item);
            }
            keylist.deinit();
        }

        // Iterate through all keys in the map
        var key_itr = self.keydir.keyIterator();
        while (key_itr.next()) |entry| {
            // Duplicate the key
            const key = try allocator.dupe(u8, entry.*);
            // Try to append, free key if append fails
            keylist.append(key) catch |err| {
                allocator.free(key);
                return err;
            };
        }

        return keylist;
    }

    pub fn sync(self: *FileDB) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.datafile.sync();
    }

    pub fn storeHashMap(self: *FileDB) !void {
        self.mu.lock();
        defer self.mu.unlock();
        try self.StoreHashMap();
    }

    fn StoreHashMap(self: *FileDB) !void {
        const path = try utils.openUserDir(self.config.dir);
        var file = try path.createFile(kd.HINTS_FILE, .{});
        defer file.close();
        var writer = file.writer();

        // Write number of hashmap entries
        try writer.writeInt(u32, @intCast(self.keydir.count()), std.builtin.Endian.little);
        var it = self.keydir.iterator();
        while (it.next()) |entry| {
            // Write key length and key
            try writer.writeInt(u32, @intCast(entry.key_ptr.*.len), std.builtin.Endian.little);
            try writer.writeAll(entry.key_ptr.*);

            const meta = entry.value_ptr.*;
            // Write number of metadata entries
            try writer.writeInt(u32, meta.file_id, std.builtin.Endian.little);
            try writer.writeInt(usize, meta.value_sz, std.builtin.Endian.little);
            try writer.writeInt(usize, meta.value_pos, std.builtin.Endian.little);
            try writer.writeInt(i64, meta.tstamp, std.builtin.Endian.little);
        }
    }

    fn loadKeyDir(self: *FileDB) !void {
        self.mu.lock();
        defer self.mu.unlock();
        const path = try utils.openUserDir(self.config.dir);

        var file = path.openFile(kd.HINTS_FILE, .{}) catch |err| {
            std.log.debug("Error opening hint file: {}", .{err});
            return;
        };
        defer file.close();
        var reader = file.reader();
        const stat = try file.stat();
        if (stat.size == 0) {
            return;
        }

        // Read number of hashmap entries
        const entry_count = try reader.readInt(u32, std.builtin.Endian.little);
        var i: u32 = 0;
        while (i < entry_count) : (i += 1) {
            // Read key length and key
            const key_len = try reader.readInt(u32, std.builtin.Endian.little);
            const key_buf = try self.allocator.alloc(u8, key_len);
            errdefer self.allocator.free(key_buf);
            try reader.readNoEof(key_buf);

            // Read metadata items
            const file_id = try reader.readInt(u32, std.builtin.Endian.little);
            const value_sz = try reader.readInt(usize, std.builtin.Endian.little);
            const value_pos = try reader.readInt(usize, std.builtin.Endian.little);
            const tstamp = try reader.readInt(i64, std.builtin.Endian.little);

            const metadata = kd.Metadata.init(file_id, value_sz, value_pos, tstamp);

            try self.keydir.put(key_buf, metadata);
        }

        return;
    }
};

fn runCompaction(self: *FileDB) !void {
    const interval: u64 = self.config.compactionInterval * time.ns_per_s;
    while (!self.should_shutdown.load(.acquire)) {
        time.sleep(interval);
        {
            self.mu.lock();
            defer self.mu.unlock();
            self.log.info("Starting compaction", .{});
            try mergeDatafiles(self);
            try self.StoreHashMap();
            self.log.info("Ended compaction", .{});
        }
    }
}
fn runSync(self: *FileDB) !void {
    const interval: u64 = self.config.syncInterval * time.ns_per_s;
    while (!self.should_shutdown.load(.acquire)) {
        time.sleep(interval);
        {
            self.mu.lock();
            defer self.mu.unlock();
            try self.datafile.sync();
        }
    }
}
fn runRotation(self: *FileDB) !void {
    const interval: u64 = self.config.dfRotationInterval * time.ns_per_s;
    while (!self.should_shutdown.load(.acquire)) {
        time.sleep(interval);
        {
            self.mu.lock();
            defer self.mu.unlock();
            try rotateDf(self);
        }
    }
}

fn rotateDf(self: *FileDB) !void {
    self.log.info("Current Datafile Size: {}", .{try self.datafile.size()});
    if (try self.datafile.size() < self.config.maxFileSize) {
        return;
    }

    self.log.info("Starting DF Rotation", .{});
    const oldId = self.datafile.id;
    try self.oldfiles.filemap.put(oldId, self.datafile);
    var dir = try utils.openUserDir(self.config.dir);
    defer dir.close();
    self.datafile = try Datafile.init(self.allocator, oldId + 1, dir);
    self.log.info("Ended DF Rotation", .{});
    return;
}
fn mergeDatafiles(self: *FileDB) !void {
    var mergeFsync = false;
    if (self.oldfiles.filemap.count() < 2) {
        return;
    }
    std.fs.cwd().makeDir("tmp") catch |err| switch (err) {
        error.PathAlreadyExists => {
            self.log.debug("Directory already exists: {s}\n", .{"tmp"});
        },
        else => return err,
    };

    var tmpDirectory = try std.fs.cwd().openDir("tmp", .{ .iterate = true });
    defer tmpDirectory.close();
    defer std.fs.cwd().deleteTree("tmp") catch |err| {
        self.log.debug("Cannot Delete TempDir: {}", .{err});
    };

    const mergedDf = try Datafile.init(self.allocator, 0, tmpDirectory);
    if (self.config.alwaysFsync) {
        mergeFsync = true;
        self.config.alwaysFsync = false;
    }

    var it = self.keydir.iterator();
    while (it.next()) |entry| {
        const keydir_record = try self.getValue(entry.key_ptr.*);
        if (keydir_record) |existing_record| {
            try self.storeKV(mergedDf, entry.key_ptr.*, existing_record);
            self.allocator.free(existing_record);
        }
    }

    var oldDir = try utils.openUserDir(self.config.dir);
    defer oldDir.close();
    var walker = try oldDir.walk(self.allocator);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind != .file) {
            continue;
        }
        if (std.mem.endsWith(u8, entry.basename, ".db")) {
            oldDir.deleteFile(entry.path) catch |err| {
                self.log.debug("Failed to remove {s}: {}\n", .{ entry.path, err });
                continue;
            };
        }
    }

    const filemap = std.AutoHashMap(u32, *Datafile).init(self.allocator);
    self.oldfiles.deinit();
    self.oldfiles = try Oldfiles.init(self.allocator, filemap);
    try self.oldfiles.initializeMap(oldDir);

    try tmpDirectory.copyFile("file_0.db", oldDir, "file_0.db", .{});
    self.datafile.deinit();
    self.datafile = mergedDf;

    if (mergeFsync) {
        try self.datafile.sync();
        self.config.alwaysFsync = true;
    }
    return;
}

test "filedbInitialized" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    var options = config.defaultOptions();
    options.dir = "/home/rajiv/projects/filedb/test_initialize";
    const filedb = try FileDB.init(allocator, options);
    time.sleep(10 * time.ns_per_s);
    defer filedb.deinit();
    try expect(true);
}

test "insert a value and get it back" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    var options = config.defaultOptions();
    options.dir = "test_3";
    const filedb = try FileDB.init(allocator, options);
    defer filedb.deinit();

    try filedb.put("key1", "value1");

    const value = try filedb.get("key1");

    try expect(value != null);
    defer if (value) |v| allocator.free(v);

    const final_value = value.?;
    std.log.debug("found value '{s}'", .{final_value});
    try std.testing.expectEqualStrings("value1", final_value);
}

test "get a value which does not exist" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    var options = config.defaultOptions();
    options.dir = "test_2";
    const filedb = try FileDB.init(allocator, options);
    defer filedb.deinit();

    const value = try filedb.get("key1");

    try expect(value == null);
}

test "list all keys" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak");
    const allocator = gpa.allocator();

    var options = config.defaultOptions();
    options.dir = "test_1";
    const filedb = try FileDB.init(allocator, options);
    defer filedb.deinit();

    try filedb.put("key1", "value1");
    try filedb.put("key2", "value1");
    try filedb.put("key3", "value1");
    try filedb.put("key4", "value1");

    const keylist = try filedb.list(allocator);
    defer {
        for (keylist.items) |v| {
            allocator.free(v);
        }
        keylist.deinit();
    }

    try std.testing.expectEqual(4, keylist.items.len);
}
