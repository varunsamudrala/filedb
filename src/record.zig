const std = @import("std");
const utils = @import("utils.zig");

pub const Record = struct {
    crc: u32,
    tstamp: i64,
    key_len: usize,
    value_len: usize,
    key: []const u8,
    value: []const u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, key: []const u8, value: []const u8) !*Record {
        const record = try allocator.create(Record); // better error handling
        const key_copy = try allocator.dupe(u8, key);
        const value_copy = try allocator.dupe(u8, value);
        record.* = .{
            .crc = utils.crc32Checksum(key),
            .tstamp = std.time.timestamp(),
            .key_len = key.len,
            .value_len = value.len,
            .key = key_copy,
            .value = value_copy,
            .allocator = allocator,
        };

        return record;
    }

    pub fn encode(self: *Record, buf: []u8) !void {
        var fbs = std.io.fixedBufferStream(buf);
        var writer = fbs.writer();

        try writer.writeInt(u32, self.crc, std.builtin.Endian.little);
        try writer.writeInt(i64, self.tstamp, std.builtin.Endian.little);
        try writer.writeInt(usize, self.key_len, std.builtin.Endian.little);
        try writer.writeInt(usize, self.value_len, std.builtin.Endian.little);
        try writer.writeAll(self.key);
        try writer.writeAll(self.value);
    }

    pub fn deinit(self: *Record) void {
        self.allocator.free(self.key);
        self.allocator.free(self.value);
        self.allocator.destroy(self);
    }
};

pub fn decodeRecord(allocator: std.mem.Allocator, buf: []u8) !*Record {
    var fbs = std.io.fixedBufferStream(buf);
    var reader = fbs.reader();

    _ = try reader.readInt(u32, std.builtin.Endian.little);
    _ = try reader.readInt(i64, std.builtin.Endian.little);
    const key_len = try reader.readInt(usize, std.builtin.Endian.little);
    const value_len = try reader.readInt(usize, std.builtin.Endian.little);
    const key = try allocator.alloc(u8, key_len);
    defer allocator.free(key);
    _ = try reader.read(key);
    const value = try allocator.alloc(u8, value_len);
    defer allocator.free(value);
    _ = try reader.read(value);
    return Record.init(allocator, key, value);
}
