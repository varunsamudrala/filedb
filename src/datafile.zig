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
};
