const std = @import("std");
const types = @import("types.zig");
const dag_mod = @import("dag.zig");
const block_mod = @import("block.zig");
const Block = block_mod.Block;
const CommittedBlock = block_mod.CommittedBlock;
const DagStore = dag_mod.DagStore;
const Hash = types.Hash;
const ValidatorId = types.ValidatorId;

/// Bullshark ordering protocol.
/// Operates in waves of 2 rounds each. Each wave has a leader whose block at the
/// even round serves as the "anchor". If >= 2f+1 blocks in the odd round reference
/// the anchor, the anchor is committed along with its entire uncommitted causal history.
pub const Bullshark = struct {
    allocator: std.mem.Allocator,
    num_validators: u8,
    /// Set of block hashes that have already been committed.
    committed_set: std.AutoArrayHashMap(Hash, void),
    /// Ordered list of committed blocks.
    committed_blocks: std.ArrayList(CommittedBlock),
    /// The highest wave that has been committed.
    last_committed_wave: ?u64,

    pub fn init(allocator: std.mem.Allocator, num_validators: u8) Bullshark {
        return .{
            .allocator = allocator,
            .num_validators = num_validators,
            .committed_set = std.AutoArrayHashMap(Hash, void).init(allocator),
            .committed_blocks = .empty,
            .last_committed_wave = null,
        };
    }

    pub fn deinit(self: *Bullshark) void {
        self.committed_set.deinit();
        self.committed_blocks.deinit(self.allocator);
    }

    /// Compute the wave number for a given round.
    pub fn wave(round: u64) u64 {
        return round / 2;
    }

    /// Compute the leader of a given wave.
    pub fn leader(w: u64, num_validators: u8) ValidatorId {
        return @intCast(w % num_validators);
    }

    /// The anchor round for a wave (the even round).
    pub fn anchorRound(w: u64) u64 {
        return w * 2;
    }

    /// The voting round for a wave (the odd round).
    pub fn votingRound(w: u64) u64 {
        return w * 2 + 1;
    }

    /// Try to commit blocks. Checks if the latest complete wave's anchor can be committed.
    /// If so, commits all uncommitted blocks in the anchor's causal history in deterministic order.
    /// Returns the number of newly committed blocks.
    pub fn tryCommit(self: *Bullshark, dag: *const DagStore, current_round: u64) !u32 {
        if (current_round < 2) return 0;

        var total_committed: u32 = 0;

        const start_wave = if (self.last_committed_wave) |lcw| lcw + 1 else 0;
        const max_wave = wave(current_round);

        var w = start_wave;
        while (w < max_wave) : (w += 1) {
            const a_round = anchorRound(w);
            const v_round = votingRound(w);
            const lead = leader(w, self.num_validators);

            // Get the anchor block (leader's block at the even round).
            const anchor = dag.getBlockByRoundAuthor(a_round, lead) orelse continue;

            // Count how many blocks in the voting round reference the anchor.
            var support_count: u8 = 0;
            for (0..self.num_validators) |i| {
                const vid: ValidatorId = @intCast(i);
                if (dag.getBlockByRoundAuthor(v_round, vid)) |voting_block| {
                    for (voting_block.parents) |parent_hash| {
                        if (std.mem.eql(u8, &parent_hash, &anchor.hash)) {
                            support_count += 1;
                            break;
                        }
                    }
                }
            }

            const quorum = types.quorumSize(self.num_validators);
            if (support_count < quorum) continue;

            // The anchor is committed! Gather its causal history.
            const history = try dag.causalHistory(anchor.hash);
            defer self.allocator.free(history);

            // Commit all blocks in the causal history that haven't been committed yet.
            for (history) |blk| {
                if (!self.committed_set.contains(blk.hash)) {
                    try self.committed_set.put(blk.hash, {});
                    try self.committed_blocks.append(self.allocator, .{
                        .block = blk,
                        .commit_round = a_round,
                    });
                    total_committed += 1;
                }
            }

            self.last_committed_wave = w;
        }

        return total_committed;
    }

    /// Get all committed blocks in order.
    pub fn getCommittedBlocks(self: *const Bullshark) []const CommittedBlock {
        return self.committed_blocks.items;
    }

    /// Check if a block hash has been committed.
    pub fn isCommitted(self: *const Bullshark, hash: Hash) bool {
        return self.committed_set.contains(hash);
    }
};

test "bullshark wave and leader" {
    try std.testing.expectEqual(@as(u64, 0), Bullshark.wave(0));
    try std.testing.expectEqual(@as(u64, 0), Bullshark.wave(1));
    try std.testing.expectEqual(@as(u64, 1), Bullshark.wave(2));
    try std.testing.expectEqual(@as(u64, 1), Bullshark.wave(3));
    try std.testing.expectEqual(@as(u64, 2), Bullshark.wave(4));

    try std.testing.expectEqual(@as(u8, 0), Bullshark.leader(0, 4));
    try std.testing.expectEqual(@as(u8, 1), Bullshark.leader(1, 4));
    try std.testing.expectEqual(@as(u8, 2), Bullshark.leader(2, 4));
    try std.testing.expectEqual(@as(u8, 3), Bullshark.leader(3, 4));
    try std.testing.expectEqual(@as(u8, 0), Bullshark.leader(4, 4));
}

test "bullshark single validator commit" {
    const allocator = std.testing.allocator;

    var dag = DagStore.init(allocator);
    defer dag.deinit();

    var bs = Bullshark.init(allocator, 1);
    defer bs.deinit();

    // Round 0 (anchor round for wave 0)
    const b0 = try block_mod.Block.create(allocator, 0, 0, "round0", &.{});
    defer b0.deinit(allocator);
    _ = try dag.addBlock(b0);

    // Round 1 (voting round for wave 0)
    const parents1 = [_]Hash{b0.hash};
    const b1 = try block_mod.Block.create(allocator, 0, 1, "round1", &parents1);
    defer b1.deinit(allocator);
    _ = try dag.addBlock(b1);

    const committed = try bs.tryCommit(&dag, 2);
    try std.testing.expectEqual(@as(u32, 1), committed);

    const cblocks = bs.getCommittedBlocks();
    try std.testing.expectEqual(@as(usize, 1), cblocks.len);
    try std.testing.expectEqual(@as(u64, 0), cblocks[0].block.round);
    try std.testing.expectEqual(@as(u8, 0), cblocks[0].block.author);
}

test "bullshark 3 validators commit" {
    const allocator = std.testing.allocator;

    var dag = DagStore.init(allocator);
    defer dag.deinit();

    var bs = Bullshark.init(allocator, 3);
    defer bs.deinit();

    // 3 validators: quorum = 1 (f=0)
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
    const r1_0 = try Block.create(allocator, 0, 1, "r1_0", &parents);
    defer r1_0.deinit(allocator);
    const r1_1 = try Block.create(allocator, 1, 1, "r1_1", &parents);
    defer r1_1.deinit(allocator);
    const r1_2 = try Block.create(allocator, 2, 1, "r1_2", &parents);
    defer r1_2.deinit(allocator);
    _ = try dag.addBlock(r1_0);
    _ = try dag.addBlock(r1_1);
    _ = try dag.addBlock(r1_2);

    const committed = try bs.tryCommit(&dag, 2);
    // Anchor is g0. Its causal history is just g0 itself (no parents).
    try std.testing.expectEqual(@as(u32, 1), committed);
    try std.testing.expect(bs.isCommitted(g0.hash));
}
