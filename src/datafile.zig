const std = @import("std");
const util = @import("utils.zig");
const filename = "file_{}.db";

pub const Datafile = struct {
    reader: std.fs.File,
    writer: std.fs.File,
    mu: std.Thread.Mutex,
    id: u32,
    offset: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, index: u32, dir: std.fs.Dir) !*Datafile {
        var file_buf: [32]u8 = undefined;
        const file_name = try std.fmt.bufPrint(&file_buf, filename, .{index});
        _ = dir.createFile(file_name, .{ .read = true, .exclusive = true }) catch |err| switch (err) {
            error.PathAlreadyExists => {
                // Open existing file
            },
            else => return err,
        };
        const file = try dir.openFile(file_name, .{ .mode = .read_write });

        const stat = try file.stat();
        const datafile = try allocator.create(Datafile);

        datafile.* = .{
            .reader = file,
            .writer = file,
            .mu = std.Thread.Mutex{},
            .id = index,
            .offset = stat.size,
            .allocator = allocator,
        };
        return datafile;
    }

    pub fn deinit(self: *Datafile) void {
        self.reader.close();

        self.allocator.destroy(self);
    }

    pub fn size(self: *Datafile) !u64 {
        const stat = self.writer.stat() catch |err| {
            std.log.debug("cannot stat file '{}': {}", .{ self.id, err });

            return err;
        };

        return stat.size;
    }

    pub fn store(self: *Datafile, buf: []const u8) !u64 {
        const sz = try self.writer.write(buf);

        const offset = self.offset;
        self.offset += sz;

        return offset;
    }

    pub fn get(self: *Datafile, buf: []u8, value_pos: usize, value_size: usize) !void {
        try self.reader.seekTo(value_pos);
        const data = try self.reader.read(buf);

        if (data != value_size) {
            return ErrorMissingData.MissingData;
        }
    }

    pub fn sync(self: *Datafile) !void {
        try self.writer.sync();
    }
};

const ErrorMissingData = error{MissingData};
