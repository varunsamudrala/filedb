const std = @import("std");
const Oldfiles = @import("oldfiles.zig").OldFiles;
const utils = @import("utils.zig");

// hashmap of key-> file_id, value_sz, value_pos, tstamp

pub const HINTS_FILE = "filedb.hints";

pub const Metadata = struct {
    file_id: u32,
    value_sz: usize,
    value_pos: usize,
    tstamp: i64,

    pub fn init(file_id: u32, value_sz: usize, value_pos: usize, tstamp: i64) Metadata {
        return .{
            .file_id = file_id,
            .value_pos = value_pos,
            .value_sz = value_sz,
            .tstamp = tstamp,
        };
    }
};
