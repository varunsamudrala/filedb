const std = @import("std");
const utils = @import("utils.zig");
const Datafile = @import("datafile.zig").Datafile;

pub const OldFiles = struct {
    filemap: std.AutoHashMap(u32, *Datafile),
    allocator: std.mem.Allocator,
    path: []const u8,

    pub fn init(allocator: std.mem.Allocator, filemap: std.AutoHashMap(u32, *Datafile), path: []const u8) !*OldFiles {
        // var filemap = std.AutoHashMap(u32, *Datafile).init(allocator);
        const oldfiles = try allocator.create(OldFiles);
        const duped_path = try allocator.dupe(u8, path);
        oldfiles.* = .{ .filemap = filemap, .allocator = allocator, .path = duped_path };
        return oldfiles;
    }

    pub fn initializeMap(self: *OldFiles) !void {
        std.debug.print("PATH: {s}\n", .{self.path});
        // _ = dir.createFile(file_name, .{ .read = true, .exclusive = true }) catch |err| switch (err) {
        //     error.PathAlreadyExists => {
        //         // Open existing file
        //     },
        //     else => return err,
        // };
        // const file = try dir.openFile(file_name, .{ .mode = .read_write });
        var dir = try std.fs.openDirAbsolute(self.path, .{ .iterate = true });
        defer dir.close();
        const filelist = try utils.listAllDatabaseFiles(self.allocator, dir);
        for (filelist.items) |entry| {
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
        self.allocator.free(self.path);
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
