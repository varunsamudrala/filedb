const std = @import("std");
const Oldfiles = @import("oldfiles.zig").OldFiles;

// hashmap of key-> file_id, value_sz, value_pos, tstamp

pub const Metadata = struct {
    file_id: u32,
    value_sz: usize,
    value_pos: usize,
    tstamp: i64,

    pub fn init(file_id: u32, value_sz: usize, value_pos: usize, tstamp: i64) Metadata {
        return .{
            .file_id = file_id,
            .value_pos = value_pos,
            .value_sz = value_sz,
            .tstamp = tstamp,
        };
    }
};

pub fn loadKeyDir(allocator: std.mem.Allocator) !std.StringHashMap(Metadata) {
    var hashmap = std.StringHashMap(Metadata).init(allocator);
    var file = std.fs.cwd().openFile("filedb.hints", .{}) catch |err| {
        std.log.debug("Error opening hint file: {}", .{err});
        return hashmap;
    };
    defer file.close();
    var reader = file.reader();
    const stat = try file.stat();
    if (stat.size == 0) {
        return hashmap;
    }

    // Read number of hashmap entries
    const entry_count = try reader.readInt(u32, std.builtin.Endian.little);
    var i: u32 = 0;
    while (i < entry_count) : (i += 1) {
        // Read key length and key
        const key_len = try reader.readInt(u32, std.builtin.Endian.little);
        const key_buf = try allocator.alloc(u8, key_len);
        errdefer allocator.free(key_buf);
        try reader.readNoEof(key_buf);

        // Read number of metadata entries

        // Read metadata items
        const file_id = try reader.readInt(u32, std.builtin.Endian.little);
        const value_sz = try reader.readInt(usize, std.builtin.Endian.little);
        const value_pos = try reader.readInt(usize, std.builtin.Endian.little);
        const tstamp = try reader.readInt(i64, std.builtin.Endian.little);

        const metadata = Metadata.init(file_id, value_sz, value_pos, tstamp);

        try hashmap.put(key_buf, metadata);
    }

    return hashmap;
}

pub fn storeHashMap(
    hashmap: *std.StringHashMap(Metadata),
) !void {
    var file = try std.fs.cwd().createFile("filedb.hints", .{});
    defer file.close();
    var writer = file.writer();

    // Write number of hashmap entries
    try writer.writeInt(u32, @intCast(hashmap.count()), std.builtin.Endian.little);
    var it = hashmap.iterator();
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
