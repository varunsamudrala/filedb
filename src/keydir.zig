const std = @import("std");

// hashmap of key-> file_id, value_sz, value_pos, tstamp

pub const metadata = struct {
    file_id: u32,
    value_sz: usize,
    value_pos: usize,
    tstamp: i64,

    pub fn init(file_id: u32, value_sz: usize, value_pos: usize, tstamp: i64) metadata {
        return .{
            .file_id = file_id,
            .value_pos = value_pos,
            .value_sz = value_sz,
            .tstamp = tstamp,
        };
    }
};
