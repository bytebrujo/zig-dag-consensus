const std = @import("std");

pub const ValidatorId = u8;

/// Maximum number of validators supported.
pub const MAX_VALIDATORS: u8 = 128;

/// Represents a set of validators as a bitset for ack tracking.
pub const ValidatorSet = struct {
    bits: u128 = 0,

    pub fn set(self: *ValidatorSet, id: ValidatorId) void {
        self.bits |= @as(u128, 1) << @intCast(id);
    }

    pub fn isSet(self: ValidatorSet, id: ValidatorId) bool {
        return (self.bits & (@as(u128, 1) << @intCast(id))) != 0;
    }

    pub fn count(self: ValidatorSet) u8 {
        return @intCast(@popCount(self.bits));
    }

    pub fn contains(self: ValidatorSet, other: ValidatorSet) bool {
        return (self.bits & other.bits) == other.bits;
    }
};

pub const Hash = [32]u8;

pub const ZERO_HASH: Hash = [_]u8{0} ** 32;

/// Quorum size for n validators: 2f+1 where f = floor((n-1)/3)
pub fn quorumSize(num_validators: u8) u8 {
    const f = (num_validators - 1) / 3;
    return 2 * f + 1;
}

test "quorum sizes" {
    try std.testing.expectEqual(@as(u8, 1), quorumSize(1));
    try std.testing.expectEqual(@as(u8, 1), quorumSize(2));
    try std.testing.expectEqual(@as(u8, 1), quorumSize(3));
    try std.testing.expectEqual(@as(u8, 3), quorumSize(4));
    try std.testing.expectEqual(@as(u8, 3), quorumSize(5));
    try std.testing.expectEqual(@as(u8, 5), quorumSize(7));
}

test "validator set" {
    var vs = ValidatorSet{};
    try std.testing.expectEqual(@as(u8, 0), vs.count());
    vs.set(0);
    try std.testing.expect(vs.isSet(0));
    try std.testing.expect(!vs.isSet(1));
    try std.testing.expectEqual(@as(u8, 1), vs.count());
    vs.set(3);
    vs.set(5);
    try std.testing.expectEqual(@as(u8, 3), vs.count());
}
