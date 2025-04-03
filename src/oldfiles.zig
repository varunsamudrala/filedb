const std = @import("std");
const utils = @import("utils.zig");
const datafile = @import("datafile.zig");

pub const OldFiles = struct {
    filelist: std.ArrayList([]const u8),
    filemap: std.AutoHashMap(u32, datafile.Datafile),
    pub fn init(allocator: std.mem.Allocator) !*OldFiles {
        const filelist = try utils.listAllDatabaseFiles(allocator);
        var filemap = std.AutoHashMap(u32, datafile.Datafile).init(allocator);
        for (filelist.items) |entry| {
            const file_id = try utils.parseIdFromFilename(entry);
            const df = try datafile.Datafile.init(file_id);
            try filemap.put(file_id, df);
        }
        const oldfiles = try allocator.create(OldFiles);
        oldfiles.* = .{ .filelist = filelist, .filemap = filemap };
        return oldfiles;
    }

    pub fn deinit(self: *OldFiles, allocator: std.mem.Allocator) void {
        for (self.filelist.items) |entry| {
            allocator.free(entry);
        }
        self.filelist.deinit();
        var keydirIterator = self.filemap.valueIterator();
        while (keydirIterator.next()) |entry| {
            entry.deinit();
        }
        self.filemap.deinit();
        allocator.destroy(self);
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
