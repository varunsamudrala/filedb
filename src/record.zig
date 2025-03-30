const std = @import("std");
const utils = @import("utils.zig");

pub const Record = struct {
    crc: u32,
    tstamp: i64,
    key_len: usize,
    value_len: usize,
    key: []const u8,
    value: []const u8,

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

    pub fn deinit(self: *Record, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
        allocator.destroy(self);
    }
};
