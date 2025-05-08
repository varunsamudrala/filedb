const std = @import("std");

pub const Options = struct {
    dir: []const u8,
    alwaysFsync: bool,
};

pub fn defaultOptions() Options {
    return .{
        .dir = ".",
        .alwaysFsync = false,
    };
}
