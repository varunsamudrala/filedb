const std = @import("std");
const Crc32 = std.hash.crc.Crc32;

pub fn listAllDatabaseFiles(allocator: std.mem.Allocator) !std.ArrayList([]const u8) {
    var dir = try std.fs.cwd().openDir(".", .{ .iterate = true });
    defer dir.close();
    var walker = try dir.walk(allocator);
    defer walker.deinit();

    var filelist = std.ArrayList([]const u8).init(allocator);

    while (try walker.next()) |entry| {
        if (std.mem.endsWith(u8, entry.basename, ".db")) {
            const f = try allocator.dupe(u8, entry.basename);
            try filelist.append(f);
        }
    }
    return filelist;
}

pub fn parseIdFromFilename(filename: []const u8) !u32 {
    const prefix = "file_";
    const suffix = ".db";
    const start_index = prefix.len;
    const end_index = std.mem.indexOf(u8, filename, suffix) orelse filename.len;

    if (start_index >= end_index) {
        std.debug.print("Invalid format\n", .{});
        return error.InvalidFormat;
    }

    // Extract the substring containing the number
    const id_str = filename[start_index..end_index];

    // Convert to integer
    const id = try std.fmt.parseInt(u32, id_str, 10);
    return id;
}

pub fn validateKV(key: []const u8, value: []const u8) !void {
    if (key.len == 0) {
        return ErrorKVValidation.KeyCannotBeEmpty;
    }

    if (key.len > MAX_KEY_LEN) {
        return ErrorKVValidation.KeyLengthExceeded;
    }

    if (value.len > MAX_VAL_LEN) {
        return ErrorKVValidation.ValueLengthExceeded;
    }
}

pub const ErrorKVValidation = error{
    KeyCannotBeEmpty,
    KeyLengthExceeded,
    ValueLengthExceeded,
};

pub fn crc32Checksum(data: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(data);
    return crc.final();
}

pub const MAX_KEY_LEN = 4294967296; // 2^32
pub const MAX_VAL_LEN = 4294967296; // 2^32
