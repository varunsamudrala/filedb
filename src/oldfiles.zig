const std = @import("std");
const utils = @import("utils.zig");
const datafile = @import("datafile.zig");

pub const OldFiles = struct {
    filelist: std.ArrayList([]const u8),
    filemap: std.AutoHashMap(u32, datafile.Datafile),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !*OldFiles {
        const dir = try utils.openUserDir(path);
        const filelist = try utils.listAllDatabaseFiles(allocator, dir);

        var filemap = std.AutoHashMap(u32, datafile.Datafile).init(allocator);
        for (filelist.items) |entry| {
            const file_id = try utils.parseIdFromFilename(entry);
            const df = try datafile.Datafile.init(file_id, path);
            try filemap.put(file_id, df);
        }
        const oldfiles = try allocator.create(OldFiles);
        oldfiles.* = .{ .filelist = filelist, .filemap = filemap, .allocator = allocator };
        return oldfiles;
    }

    pub fn deinit(self: *OldFiles) void {
        for (self.filelist.items) |entry| {
            self.allocator.free(entry);
        }
        self.filelist.deinit();
        var keydirIterator = self.filemap.valueIterator();
        while (keydirIterator.next()) |entry| {
            entry.deinit();
        }
        self.filemap.deinit();
        self.allocator.destroy(self);
    }

    pub fn get(self: OldFiles, buf: []u8, file_id: u32, value_pos: usize, value_size: usize) !void {
        const df = self.filemap.get(file_id);
        if (df == null) {
            return error.EmptyDatafile;
        }

        return df.?.get(buf, value_pos, value_size);
    }
};

const ErrorMissingData = error{MissingData};
