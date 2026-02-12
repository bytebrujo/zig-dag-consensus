const std = @import("std");
const types = @import("types.zig");
const ValidatorId = types.ValidatorId;
const Hash = types.Hash;

pub const Block = struct {
    author: ValidatorId,
    round: u64,
    payload: []const u8,
    parents: []const Hash,
    hash: Hash,

    /// Compute the Blake3 hash of a block (excluding the hash field itself).
    pub fn computeHash(author: ValidatorId, round: u64, payload: []const u8, parents: []const Hash) Hash {
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update(&[_]u8{author});
        hasher.update(std.mem.asBytes(&round));
        hasher.update(payload);
        for (parents) |p| {
            hasher.update(&p);
        }
        var result: Hash = undefined;
        hasher.final(&result);
        return result;
    }

    /// Create a new block with its hash computed.
    pub fn create(
        allocator: std.mem.Allocator,
        author: ValidatorId,
        round: u64,
        payload: []const u8,
        parents: []const Hash,
    ) !Block {
        const owned_payload = try allocator.dupe(u8, payload);
        errdefer allocator.free(owned_payload);
        const owned_parents = try allocator.dupe(Hash, parents);
        errdefer allocator.free(owned_parents);

        const hash = computeHash(author, round, payload, parents);

        return Block{
            .author = author,
            .round = round,
            .payload = owned_payload,
            .parents = owned_parents,
            .hash = hash,
        };
    }

    /// Free memory owned by this block.
    pub fn deinit(self: Block, allocator: std.mem.Allocator) void {
        if (self.payload.len > 0) allocator.free(self.payload);
        if (self.parents.len > 0) allocator.free(self.parents);
    }

    /// Clone a block, duplicating all owned slices.
    pub fn clone(self: Block, allocator: std.mem.Allocator) !Block {
        const p = try allocator.dupe(u8, self.payload);
        errdefer allocator.free(p);
        const par = try allocator.dupe(Hash, self.parents);
        return Block{
            .author = self.author,
            .round = self.round,
            .payload = p,
            .parents = par,
            .hash = self.hash,
        };
    }

    /// Verify that the hash matches the block contents.
    pub fn verifyHash(self: Block) bool {
        const expected = computeHash(self.author, self.round, self.payload, self.parents);
        return std.mem.eql(u8, &expected, &self.hash);
    }
};

pub const CommittedBlock = struct {
    block: Block,
    commit_round: u64,
};

test "block hash determinism" {
    const parents = [_]Hash{
        [_]u8{1} ** 32,
        [_]u8{2} ** 32,
    };
    const h1 = Block.computeHash(0, 1, "hello", &parents);
    const h2 = Block.computeHash(0, 1, "hello", &parents);
    try std.testing.expectEqualSlices(u8, &h1, &h2);

    // Different author -> different hash
    const h3 = Block.computeHash(1, 1, "hello", &parents);
    try std.testing.expect(!std.mem.eql(u8, &h1, &h3));
}

test "block create and verify" {
    const allocator = std.testing.allocator;
    const parents = [_]Hash{[_]u8{0xAA} ** 32};
    const blk = try Block.create(allocator, 2, 5, "tx_data", &parents);
    defer blk.deinit(allocator);

    try std.testing.expectEqual(@as(u8, 2), blk.author);
    try std.testing.expectEqual(@as(u64, 5), blk.round);
    try std.testing.expect(blk.verifyHash());
}
