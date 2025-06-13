// const std = @import("std");
// const FileDB = @import("filedb.zig").FileDB;
// const FileDBConfig = @import("config.zig");

// pub fn main() !void {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     const options = FileDBConfig.defaultOptions();
//     const filedb = try FileDB.init(allocator, options);

//     var listener = try std.net.Address.listen(try std.net.Address.parseIp("0.0.0.0", 6379), .{
//         .reuse_address = true,
//     });

//     filedb.log.debug("Listening on 0.0.0.0:6379\n", .{});

//     while (true) {
//         const conn = try listener.accept();
//         _ = try std.Thread.spawn(.{}, handleClient, .{ conn.stream, filedb });
//     }
// }

// fn readRespCommand(reader: anytype, allocator: std.mem.Allocator) ![][]const u8 {
//     var buf1: [128]u8 = undefined;
//     const first_line = try reader.readUntilDelimiterOrEof(&buf1, '\n') orelse return error.EndOfStream;

//     if (first_line.len < 1 or first_line[0] != '*') return error.InvalidCommand;

//     const argc = try std.fmt.parseInt(usize, std.mem.trimRight(u8, first_line[1..], "\r"), 10);
//     var args = try allocator.alloc([]const u8, argc);

//     var i: usize = 0;
//     while (i < argc) {
//         var len_buf: [128]u8 = undefined;
//         const len_line = try reader.readUntilDelimiterOrEof(&len_buf, '\n') orelse return error.UnexpectedEof;
//         if (len_line.len < 1 or len_line[0] != '$') return error.InvalidCommand;

//         const len = try std.fmt.parseInt(usize, std.mem.trimRight(u8, len_line[1..], "\r"), 10);
//         const data = try allocator.alloc(u8, len);
//         _ = try reader.readNoEof(data);

//         var crlf: [2]u8 = undefined;
//         _ = try reader.readNoEof(&crlf);
//         if (!(crlf[0] == '\r' and crlf[1] == '\n')) return error.InvalidLineEnding;

//         args[i] = data;
//         i += 1;
//     }

//     return args;
// }

// fn handleClient(stream: std.net.Stream, db: *FileDB) !void {
//     const reader = stream.reader();
//     const writer = stream.writer();

//     while (true) {
//         std.debug.print("===> parsing next RESP command\n", .{});
//         const args = readRespCommand(reader, std.heap.page_allocator) catch |err| {
//             try writer.writeAll("-ERR parse error\r\n");
//             std.debug.print("error: {}", .{err});
//             break;
//         };

//         // args[0] is the command like "GET"
//         const cmd = std.ascii.allocUpperString(std.heap.page_allocator, args[0]) catch args[0];

//         if (std.mem.eql(u8, cmd, "PING")) {
//             try writer.writeAll("+PONG\r\n");
//         } else if (std.mem.eql(u8, cmd, "SET") and args.len == 3) {
//             try db.put(args[1], args[2]);
//             try writer.writeAll("+OK\r\n");
//         } else if (std.mem.eql(u8, cmd, "GET") and args.len == 2) {
//             if (try db.get(args[1])) |val| {
//                 try writer.print("${d}\r\n{s}\r\n", .{ val.len, val });
//             } else {
//                 try writer.writeAll("$-1\r\n");
//             }
//         } else if (std.mem.eql(u8, cmd, "DEL") and args.len == 2) {
//             try db.delete(args[1]);
//             try writer.writeAll(":1\r\n"); // integer reply (like Redis DEL)
//         } else if (std.mem.eql(u8, cmd, "CONFIG")) {
//             try writer.writeAll("*0\r\n"); // empty response
//         } else {
//             try writer.writeAll("-ERR unknown command\r\n");
//         }

//         // Free memory
//         for (args) |item| {
//             std.heap.page_allocator.free(item);
//         }
//         std.heap.page_allocator.free(args);
//     }
// }

const std = @import("std");
const FileDB = @import("filedb.zig").FileDB;
const FileDBConfig = @import("config.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var options = FileDBConfig.defaultOptions();
    options.alwaysFsync = false;
    options.compactionInterval = 100;
    options.dfRotationInterval = 150;
    options.maxFileSize = 1000 * 1024 * 1024;
    options.log_level = std.log.Level.info;
    var filedb = try FileDB.init(allocator, options);
    defer filedb.deinit(); // Assuming FileDB has a deinit method

    var listener = try std.net.Address.listen(try std.net.Address.parseIp("0.0.0.0", 6379), .{
        .reuse_address = true,
    });
    defer listener.deinit();

    filedb.log.debug("Listening on 0.0.0.0:6379\n", .{});

    while (true) {
        const conn = listener.accept() catch |err| {
            filedb.log.debug("Accept error: {}\n", .{err});
            continue;
        };

        // Create a context struct to pass both the connection and database
        const context = try allocator.create(ClientContext);
        context.* = ClientContext{
            .stream = conn.stream,
            .db = filedb,
            .allocator = allocator,
        };

        _ = try std.Thread.spawn(.{}, handleClient, .{context});
    }
}

fn readRespCommand(reader: anytype, allocator: std.mem.Allocator) ![][]const u8 {
    var buf1: [128]u8 = undefined;
    const first_line = try reader.readUntilDelimiterOrEof(&buf1, '\n') orelse return error.EndOfStream;

    if (first_line.len < 1 or first_line[0] != '*') return error.InvalidCommand;

    const argc = try std.fmt.parseInt(usize, std.mem.trimRight(u8, first_line[1..], "\r"), 10);
    var args = try allocator.alloc([]const u8, argc);
    errdefer {
        // Clean up partially allocated args on error
        for (args[0..]) |arg| {
            allocator.free(arg);
        }
        allocator.free(args);
    }

    var i: usize = 0;
    while (i < argc) {
        var len_buf: [128]u8 = undefined;
        const len_line = try reader.readUntilDelimiterOrEof(&len_buf, '\n') orelse return error.UnexpectedEof;

        if (len_line.len < 1 or len_line[0] != '$') return error.InvalidCommand;

        const len = try std.fmt.parseInt(usize, std.mem.trimRight(u8, len_line[1..], "\r"), 10);
        const data = try allocator.alloc(u8, len);
        _ = try reader.readNoEof(data);

        var crlf: [2]u8 = undefined;
        _ = try reader.readNoEof(&crlf);
        if (!(crlf[0] == '\r' and crlf[1] == '\n')) return error.InvalidLineEnding;

        args[i] = data;
        i += 1;
    }

    return args;
}

const ClientContext = struct {
    stream: std.net.Stream,
    db: *FileDB,
    allocator: std.mem.Allocator,
};

fn handleClient(context: *ClientContext) !void {
    defer {
        context.stream.close();
        context.allocator.destroy(context);
    }

    const reader = context.stream.reader();
    const writer = context.stream.writer();

    while (true) {
        context.db.log.debug("===> parsing next RESP command\n", .{});

        const args = readRespCommand(reader, context.allocator) catch |err| {
            switch (err) {
                error.EndOfStream => {
                    context.db.log.debug("Client disconnected\n", .{});
                    break;
                },
                else => {
                    try writer.writeAll("-ERR parse error\r\n");
                    context.db.log.debug("Parse error: {}\n", .{err});
                    continue; // Try to continue processing instead of breaking
                },
            }
        };

        defer {
            // Free memory after processing
            for (args) |item| {
                context.allocator.free(item);
            }
            context.allocator.free(args);
        }

        if (args.len == 0) continue;

        // args[0] is the command like "GET"
        const cmd = std.ascii.allocUpperString(context.allocator, args[0]) catch args[0];
        defer if (cmd.ptr != args[0].ptr) context.allocator.free(cmd);

        if (std.mem.eql(u8, cmd, "PING")) {
            try writer.writeAll("+PONG\r\n");
        } else if (std.mem.eql(u8, cmd, "SET") and args.len == 3) {
            try context.db.put(args[1], args[2]);
            try writer.writeAll("+OK\r\n");
        } else if (std.mem.eql(u8, cmd, "GET") and args.len == 2) {
            if (try context.db.get(args[1])) |val| {
                try writer.print("${d}\r\n{s}\r\n", .{ val.len, val });
            } else {
                try writer.writeAll("$-1\r\n");
            }
        } else if (std.mem.eql(u8, cmd, "DEL") and args.len == 2) {
            try context.db.delete(args[1]);
            try writer.writeAll(":1\r\n"); // integer reply (like Redis DEL)
        } else if (std.mem.eql(u8, cmd, "CONFIG")) {
            // Handle CONFIG commands more properly
            if (args.len >= 2 and std.mem.eql(u8, std.ascii.allocUpperString(context.allocator, args[1]) catch args[1], "GET")) {
                // Return empty array for any CONFIG GET request
                try writer.writeAll("*0\r\n");
            } else {
                try writer.writeAll("*0\r\n"); // empty response for other CONFIG commands
            }
        } else {
            try writer.writeAll("-ERR unknown command\r\n");
        }
    }
}
