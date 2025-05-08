const std = @import("std");

const filename = "file_{}.db";

pub const Datafile = struct {
    reader: std.fs.File,
    writer: std.fs.File,
    mu: std.Thread.Mutex,
    id: u32,
    offset: u64,

    pub fn init(index: u32) !Datafile {
        var file_buf: [32]u8 = undefined;
        const file = try std.fmt.bufPrint(&file_buf, filename, .{index});
        const writer = std.fs.cwd().createFile(file, .{}) catch |err| {
            std.debug.print("Failed to create file '{s}': {}\n", .{ file, err });
            return err;
        };

        const reader = std.fs.cwd().openFile(file, .{}) catch |err| {
            std.debug.print("Failed to open file for reading '{s}': {}\n", .{ file, err });
            return err;
        };

        const stat = try writer.stat();

        return Datafile{
            .reader = reader,
            .writer = writer,
            .mu = std.Thread.Mutex{},
            .id = index,
            .offset = stat.size,
        };
    }

    pub fn deinit(self: Datafile) void {
        self.reader.close();
        self.writer.close();
    }

    pub fn size(self: Datafile) !u64 {
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

    pub fn get(self: Datafile, buf: []u8, value_pos: usize, value_size: usize) !void {
        try self.reader.seekTo(value_pos);
        const data = try self.reader.read(buf);

        if (data != value_size) {
            return ErrorMissingData.MissingData;
        }
    }

    pub fn sync(self: Datafile) !void {
        try self.writer.sync();
    }
};

const ErrorMissingData = error{MissingData};
