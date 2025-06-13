const std = @import("std");
const utils = @import("utils.zig");
const Datafile = @import("datafile.zig").Datafile;

pub const OldFiles = struct {
    filemap: std.AutoHashMap(u32, *Datafile),
    allocator: std.mem.Allocator,
    // path: []const u8,

    pub fn init(allocator: std.mem.Allocator, filemap: std.AutoHashMap(u32, *Datafile)) !*OldFiles {
        const oldfiles = try allocator.create(OldFiles);
        oldfiles.* = .{ .filemap = filemap, .allocator = allocator };
        return oldfiles;
    }

    pub fn initializeMap(self: *OldFiles, dir: std.fs.Dir) !void {
        const filelist = try utils.listAllDatabaseFiles(self.allocator, dir);
        for (filelist.items) |entry| {
            std.debug.print("Found file: {s}", .{entry});
            const file_id = try utils.parseIdFromFilename(entry);
            const df = try Datafile.init(self.allocator, file_id, dir);
            try self.filemap.put(file_id, df);
        }
        for (filelist.items) |entry| {
            self.allocator.free(entry);
        }
        filelist.deinit();
    }

    pub fn deinit(self: *OldFiles) void {
        var keydirIterator = self.filemap.iterator();
        while (keydirIterator.next()) |entry| {
            entry.value_ptr.*.deinit();
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
