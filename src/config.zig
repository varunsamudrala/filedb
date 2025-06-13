const std = @import("std");
const log = std.log;

pub const Options = struct {
    dir: []const u8,
    alwaysFsync: bool,
    log_level: log.Level,
    maxFileSize: u32,
    compactionInterval: u64, // in seconds
    dfRotationInterval: u64,
    syncInterval: u64,
};

pub fn defaultOptions() Options {
    return .{
        .dir = "/home/rajiv/projects/filedb/filedb",
        .alwaysFsync = false,
        .log_level = log.Level.info,
        .maxFileSize = 10 * 1024 * 1024, //10mb
        .compactionInterval = 10,
        .dfRotationInterval = 15,
        .syncInterval = 15,
    };
}
