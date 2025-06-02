const std = @import("std");
const log = std.log;

pub const Logger = struct {
    level: log.Level,

    const Self = @This();

    pub fn init(level: log.Level) Self {
        return Self{
            .level = level,
        };
    }

    // Helper method to check if a log level should be printed
    fn shouldLog(self: *const Self, level: log.Level) bool {
        return @intFromEnum(level) <= @intFromEnum(self.level);
    }

    // Custom logging methods
    pub fn info(self: *const Self, comptime format: []const u8, args: anytype) void {
        if (self.shouldLog(.info)) {
            log.info(format, args);
        }
    }

    pub fn debug(self: *const Self, comptime format: []const u8, args: anytype) void {
        if (self.shouldLog(.debug)) {
            log.debug(format, args);
        }
    }

    pub fn warn(self: *const Self, comptime format: []const u8, args: anytype) void {
        if (self.shouldLog(.warn)) {
            log.warn(format, args);
        }
    }

    pub fn err(self: *const Self, comptime format: []const u8, args: anytype) void {
        if (self.shouldLog(.err)) {
            log.err(format, args);
        }
    }
};
