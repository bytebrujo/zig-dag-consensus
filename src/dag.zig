const std = @import("std");
const types = @import("types.zig");
const block_mod = @import("block.zig");
const Block = block_mod.Block;
const Hash = types.Hash;
const ValidatorId = types.ValidatorId;
const ValidatorSet = types.ValidatorSet;

/// Key for indexing blocks by (round, author).
pub const RoundAuthor = struct {
    round: u64,
    author: ValidatorId,
};

/// Storage for DAG blocks, indexed by hash and by (round, author).
pub const DagStore = struct {
    allocator: std.mem.Allocator,

    /// All blocks by hash. Blocks stored here are owned by the DagStore.
    blocks_by_hash: std.AutoArrayHashMap(Hash, Block),

    /// Index: (round, author) -> hash
    blocks_by_ra: std.AutoArrayHashMap(RoundAuthor, Hash),

    /// Ack tracking: block hash -> set of validators that acked it.
    acks: std.AutoArrayHashMap(Hash, ValidatorSet),

    /// Maximum round seen so far.
    max_round: u64,

    pub fn init(allocator: std.mem.Allocator) DagStore {
        return .{
            .allocator = allocator,
            .blocks_by_hash = std.AutoArrayHashMap(Hash, Block).init(allocator),
            .blocks_by_ra = std.AutoArrayHashMap(RoundAuthor, Hash).init(allocator),
            .acks = std.AutoArrayHashMap(Hash, ValidatorSet).init(allocator),
            .max_round = 0,
        };
    }

    pub fn deinit(self: *DagStore) void {
        for (self.blocks_by_hash.values()) |blk| {
            blk.deinit(self.allocator);
        }
        self.blocks_by_hash.deinit();
        self.blocks_by_ra.deinit();
        self.acks.deinit();
    }

    /// Add a block to the DAG. Returns true if it was newly inserted, false if already present.
    pub fn addBlock(self: *DagStore, blk: Block) !bool {
        const gop = try self.blocks_by_hash.getOrPut(blk.hash);
        if (gop.found_existing) {
            return false;
        }
        const owned = try blk.clone(self.allocator);
        gop.value_ptr.* = owned;

        try self.blocks_by_ra.put(.{ .round = blk.round, .author = blk.author }, blk.hash);

        if (blk.round > self.max_round) {
            self.max_round = blk.round;
        }

        // Auto-ack from author
        const ack_gop = try self.acks.getOrPut(blk.hash);
        if (!ack_gop.found_existing) {
            ack_gop.value_ptr.* = ValidatorSet{};
        }
        ack_gop.value_ptr.set(blk.author);

        return true;
    }

    /// Get a block by its hash.
    pub fn getBlock(self: *const DagStore, hash: Hash) ?Block {
        return self.blocks_by_hash.get(hash);
    }

    /// Check if a block exists by its hash.
    pub fn hasBlock(self: *const DagStore, hash: Hash) bool {
        return self.blocks_by_hash.contains(hash);
    }

    /// Get the block at (round, author), if it exists.
    pub fn getBlockByRoundAuthor(self: *const DagStore, round: u64, author: ValidatorId) ?Block {
        const hash = self.blocks_by_ra.get(.{ .round = round, .author = author }) orelse return null;
        return self.blocks_by_hash.get(hash);
    }

    /// Get all blocks at a given round.
    pub fn getBlocksAtRound(self: *const DagStore, round: u64, num_validators: u8) ![]Block {
        var result: std.ArrayList(Block) = .empty;
        defer result.deinit(self.allocator);
        for (0..num_validators) |i| {
            const vid: ValidatorId = @intCast(i);
            if (self.getBlockByRoundAuthor(round, vid)) |blk| {
                try result.append(self.allocator, blk);
            }
        }
        return try result.toOwnedSlice(self.allocator);
    }

    /// Record an acknowledgement for a block from a validator.
    pub fn addAck(self: *DagStore, block_hash: Hash, from: ValidatorId) !void {
        const gop = try self.acks.getOrPut(block_hash);
        if (!gop.found_existing) {
            gop.value_ptr.* = ValidatorSet{};
        }
        gop.value_ptr.set(from);
    }

    /// Get the number of acks for a block.
    pub fn ackCount(self: *const DagStore, block_hash: Hash) u8 {
        if (self.acks.get(block_hash)) |vs| {
            return vs.count();
        }
        return 0;
    }

    /// Check if a block is certified (has >= quorum acks).
    pub fn isCertified(self: *const DagStore, block_hash: Hash, num_validators: u8) bool {
        return self.ackCount(block_hash) >= types.quorumSize(num_validators);
    }

    /// Count certified blocks at a given round.
    pub fn certifiedCountAtRound(self: *const DagStore, round: u64, num_validators: u8) u8 {
        var count: u8 = 0;
        for (0..num_validators) |i| {
            const vid: ValidatorId = @intCast(i);
            if (self.blocks_by_ra.get(.{ .round = round, .author = vid })) |hash| {
                if (self.isCertified(hash, num_validators)) {
                    count += 1;
                }
            }
        }
        return count;
    }

    /// Collect the causal history of a block (all ancestors reachable through parent links),
    /// including the block itself. Returns blocks in deterministic order: sorted by (round, author).
    pub fn causalHistory(self: *const DagStore, start_hash: Hash) ![]Block {
        var visited = std.AutoArrayHashMap(Hash, void).init(self.allocator);
        defer visited.deinit();

        var stack: std.ArrayList(Hash) = .empty;
        defer stack.deinit(self.allocator);

        try stack.append(self.allocator, start_hash);
        try visited.put(start_hash, {});

        while (stack.items.len > 0) {
            const h = stack.pop().?;
            if (self.blocks_by_hash.get(h)) |blk| {
                for (blk.parents) |parent_hash| {
                    if (!visited.contains(parent_hash)) {
                        try visited.put(parent_hash, {});
                        if (self.blocks_by_hash.contains(parent_hash)) {
                            try stack.append(self.allocator, parent_hash);
                        }
                    }
                }
            }
        }

        // Collect all visited blocks that exist in our store
        var result: std.ArrayList(Block) = .empty;
        defer result.deinit(self.allocator);
        for (visited.keys()) |h| {
            if (self.blocks_by_hash.get(h)) |blk| {
                try result.append(self.allocator, blk);
            }
        }

        // Sort by (round, author) for deterministic ordering.
        std.mem.sortUnstable(Block, result.items, {}, struct {
            fn lessThan(_: void, a: Block, b: Block) bool {
                if (a.round != b.round) return a.round < b.round;
                return a.author < b.author;
            }
        }.lessThan);

        return try result.toOwnedSlice(self.allocator);
    }
};

test "dag add and retrieve" {
    const allocator = std.testing.allocator;
    var dag = DagStore.init(allocator);
    defer dag.deinit();

    const blk = try Block.create(allocator, 0, 0, "data", &.{});
    defer blk.deinit(allocator);

    const added = try dag.addBlock(blk);
    try std.testing.expect(added);

    const added2 = try dag.addBlock(blk);
    try std.testing.expect(!added2);

    const retrieved = dag.getBlock(blk.hash);
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqual(blk.author, retrieved.?.author);
    try std.testing.expectEqual(blk.round, retrieved.?.round);

    const by_ra = dag.getBlockByRoundAuthor(0, 0);
    try std.testing.expect(by_ra != null);
}

test "dag causal history" {
    const allocator = std.testing.allocator;
    var dag = DagStore.init(allocator);
    defer dag.deinit();

    const g0 = try Block.create(allocator, 0, 0, "g0", &.{});
    defer g0.deinit(allocator);
    const g1 = try Block.create(allocator, 1, 0, "g1", &.{});
    defer g1.deinit(allocator);
    const g2 = try Block.create(allocator, 2, 0, "g2", &.{});
    defer g2.deinit(allocator);

    _ = try dag.addBlock(g0);
    _ = try dag.addBlock(g1);
    _ = try dag.addBlock(g2);

    const parents = [_]Hash{ g0.hash, g1.hash, g2.hash };
    const b1 = try Block.create(allocator, 0, 1, "b1", &parents);
    defer b1.deinit(allocator);
    _ = try dag.addBlock(b1);

    const history = try dag.causalHistory(b1.hash);
    defer allocator.free(history);
    try std.testing.expectEqual(@as(usize, 4), history.len);
    try std.testing.expectEqual(@as(u64, 0), history[0].round);
    try std.testing.expectEqual(@as(u8, 0), history[0].author);
    try std.testing.expectEqual(@as(u64, 0), history[1].round);
    try std.testing.expectEqual(@as(u8, 1), history[1].author);
    try std.testing.expectEqual(@as(u64, 0), history[2].round);
    try std.testing.expectEqual(@as(u8, 2), history[2].author);
    try std.testing.expectEqual(@as(u64, 1), history[3].round);
}

test "dag ack tracking" {
    const allocator = std.testing.allocator;
    var dag = DagStore.init(allocator);
    defer dag.deinit();

    const blk = try Block.create(allocator, 0, 0, "data", &.{});
    defer blk.deinit(allocator);
    _ = try dag.addBlock(blk);

    try std.testing.expectEqual(@as(u8, 1), dag.ackCount(blk.hash));

    try dag.addAck(blk.hash, 1);
    try dag.addAck(blk.hash, 2);
    try std.testing.expectEqual(@as(u8, 3), dag.ackCount(blk.hash));

    try std.testing.expect(dag.isCertified(blk.hash, 4));
    try std.testing.expect(dag.isCertified(blk.hash, 5));
}
