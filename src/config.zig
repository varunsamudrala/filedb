const std = @import("std");
const log = std.log;

pub const Options = struct {
    dir: []const u8,
    alwaysFsync: bool,
    log_level: log.Level,
};

pub fn defaultOptions() Options {
    return .{
        .dir = "/home/rajiv/projects/filedb/filedb",
        .alwaysFsync = false,
        .log_level = log.Level.info,
    };
}