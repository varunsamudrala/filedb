const std = @import("std");

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
            std.debug.print("filename: {s}\n", .{entry.basename});
        }
    }
    return filelist;
}
