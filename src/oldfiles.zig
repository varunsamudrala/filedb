const std = @import("std");
const utils = @import("utils.zig");
pub const OldFiles = struct {
    filelist: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) !OldFiles {
        const filelist = try utils.listAllDatabaseFiles(allocator);
        for (filelist.items) |file| {
            std.debug.print("file: {s}\n", .{file});
        }
        return .{ .filelist = filelist };
    }

    pub fn deinit(self: OldFiles) void {
        for (self.filelist.items) |file| {
            self.filelist.allocator.free(file);
        }
        self.filelist.deinit();
    }
};
