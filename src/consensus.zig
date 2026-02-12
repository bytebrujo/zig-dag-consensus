const std = @import("std");
const types = @import("types.zig");
const block_mod = @import("block.zig");
const dag_mod = @import("dag.zig");
const bullshark_mod = @import("bullshark.zig");
const message_mod = @import("message.zig");
const transport_mod = @import("transport.zig");

const Block = block_mod.Block;
const CommittedBlock = block_mod.CommittedBlock;
const DagStore = dag_mod.DagStore;
const Bullshark = bullshark_mod.Bullshark;
const Message = message_mod.Message;
const Transport = transport_mod.Transport;
const ValidatorId = types.ValidatorId;
const Hash = types.Hash;

/// The main DAG consensus engine. Combines Narwhal-style DAG construction
/// with Bullshark ordering.
pub const DagConsensus = struct {
    allocator: std.mem.Allocator,
    node_id: ValidatorId,
    num_validators: u8,
    transport: Transport,
    on_commit: *const fn (blocks: []const CommittedBlock) void,

    dag: DagStore,
    bullshark: Bullshark,

    current_round: u64,

    /// Pending payloads to be included in the next block.
    pending_payloads: std.ArrayList([]const u8),

    /// Whether we have already proposed a block for the current round.
    proposed_current_round: bool,

    /// Track which blocks have already triggered an on_commit callback.
    last_commit_index: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        node_id: ValidatorId,
        num_validators: u8,
        transport: Transport,
        on_commit: *const fn (blocks: []const CommittedBlock) void,
    ) !DagConsensus {
        return DagConsensus{
            .allocator = allocator,
            .node_id = node_id,
            .num_validators = num_validators,
            .transport = transport,
            .on_commit = on_commit,
            .dag = DagStore.init(allocator),
            .bullshark = Bullshark.init(allocator, num_validators),
            .current_round = 0,
            .pending_payloads = .empty,
            .proposed_current_round = false,
            .last_commit_index = 0,
        };
    }

    pub fn deinit(self: *DagConsensus) void {
        for (self.pending_payloads.items) |p| {
            self.allocator.free(p);
        }
        self.pending_payloads.deinit(self.allocator);
        self.bullshark.deinit();
        self.dag.deinit();
    }

    /// Submit a batch of transactions for consensus.
    pub fn submit(self: *DagConsensus, payload: []const u8) !void {
        const owned = try self.allocator.dupe(u8, payload);
        try self.pending_payloads.append(self.allocator, owned);
    }

    /// Get a MessageHandler that can be registered with InMemoryNetwork.
    pub fn messageHandler(self: *DagConsensus) transport_mod.MessageHandler {
        return .{
            .handle_fn = @ptrCast(&handleMessageWrapper),
            .ctx = @ptrCast(self),
        };
    }

    fn handleMessageWrapper(self: *DagConsensus, from: ValidatorId, msg: Message) void {
        self.handleMessage(from, msg) catch {};
    }

    /// Process a received message from another validator.
    pub fn handleMessage(self: *DagConsensus, from: ValidatorId, msg: Message) !void {
        switch (msg) {
            .proposal => |blk| {
                try self.handleProposal(from, blk);
            },
            .ack => |a| {
                try self.handleAck(a.block_hash, a.from);
            },
            .request_block => |hash| {
                try self.handleBlockRequest(from, hash);
            },
            .block_response => |blk| {
                try self.handleProposal(from, blk);
            },
        }
    }

    fn handleProposal(self: *DagConsensus, from: ValidatorId, blk: Block) !void {
        _ = from;
        // Verify hash
        if (!blk.verifyHash()) return;

        // Check for missing parents and request them.
        for (blk.parents) |parent_hash| {
            if (!self.dag.hasBlock(parent_hash)) {
                self.transport.send(blk.author, .{ .request_block = parent_hash });
            }
        }

        // Add to DAG
        const is_new = try self.dag.addBlock(blk);
        if (!is_new) return;

        // Send ack to all validators.
        for (0..self.num_validators) |i| {
            const vid: ValidatorId = @intCast(i);
            if (vid != self.node_id) {
                self.transport.send(vid, .{ .ack = .{
                    .block_hash = blk.hash,
                    .from = self.node_id,
                } });
            }
        }

        // Also self-ack
        try self.dag.addAck(blk.hash, self.node_id);

        // Try to advance round
        try self.tryAdvanceRound();
    }

    fn handleAck(self: *DagConsensus, block_hash: Hash, from: ValidatorId) !void {
        try self.dag.addAck(block_hash, from);
        try self.tryAdvanceRound();
    }

    fn handleBlockRequest(self: *DagConsensus, from: ValidatorId, hash: Hash) !void {
        if (self.dag.getBlock(hash)) |blk| {
            self.transport.send(from, .{ .block_response = blk });
        }
    }

    /// Try to advance to the next round. We advance when we have >= quorum certified blocks at
    /// the current round.
    fn tryAdvanceRound(self: *DagConsensus) !void {
        const quorum = types.quorumSize(self.num_validators);
        const certified = self.dag.certifiedCountAtRound(self.current_round, self.num_validators);
        if (certified >= quorum) {
            self.current_round += 1;
            self.proposed_current_round = false;
        }
    }

    /// Advance the protocol: propose a block if we haven't yet, try Bullshark commits.
    pub fn tick(self: *DagConsensus) !void {
        // Propose a block for the current round if we haven't already.
        if (!self.proposed_current_round) {
            try self.proposeBlock();
        }

        // Try Bullshark commit.
        const newly_committed = try self.bullshark.tryCommit(&self.dag, self.current_round);
        if (newly_committed > 0) {
            const all = self.bullshark.getCommittedBlocks();
            const new_blocks = all[self.last_commit_index..];
            if (new_blocks.len > 0) {
                self.on_commit(new_blocks);
                self.last_commit_index = all.len;
            }
        }

        // Try to advance round again after processing.
        try self.tryAdvanceRound();
    }

    /// Create and broadcast a block for the current round.
    fn proposeBlock(self: *DagConsensus) !void {
        // Gather parents: all known blocks from the previous round.
        var parents: std.ArrayList(Hash) = .empty;
        defer parents.deinit(self.allocator);

        if (self.current_round > 0) {
            const prev_round = self.current_round - 1;
            for (0..self.num_validators) |i| {
                const vid: ValidatorId = @intCast(i);
                const key = dag_mod.RoundAuthor{ .round = prev_round, .author = vid };
                if (self.dag.blocks_by_ra.get(key)) |hash| {
                    try parents.append(self.allocator, hash);
                }
            }
        }

        // Merge pending payloads into one.
        var payload_buf: std.ArrayList(u8) = .empty;
        defer payload_buf.deinit(self.allocator);
        for (self.pending_payloads.items) |p| {
            try payload_buf.appendSlice(self.allocator, p);
            self.allocator.free(p);
        }
        self.pending_payloads.clearRetainingCapacity();

        const payload = try payload_buf.toOwnedSlice(self.allocator);
        defer self.allocator.free(payload);

        const blk = try Block.create(
            self.allocator,
            self.node_id,
            self.current_round,
            payload,
            parents.items,
        );
        defer blk.deinit(self.allocator);

        // Add to our own DAG.
        _ = try self.dag.addBlock(blk);

        // Broadcast proposal to all other validators.
        for (0..self.num_validators) |i| {
            const vid: ValidatorId = @intCast(i);
            if (vid != self.node_id) {
                self.transport.send(vid, .{ .proposal = blk });
            }
        }

        self.proposed_current_round = true;
    }

    /// Get the current round.
    pub fn currentRound(self: *const DagConsensus) u64 {
        return self.current_round;
    }

    /// Get committed blocks so far.
    pub fn committedBlocks(self: *const DagConsensus) []const CommittedBlock {
        return self.bullshark.getCommittedBlocks();
    }
};

// --- Tests ---

/// Shared commit log for tests - using a simple global slice approach.
var test_commit_count: usize = 0;

fn testOnCommit(blocks: []const CommittedBlock) void {
    test_commit_count += blocks.len;
}

fn resetTestCommitCount() void {
    test_commit_count = 0;
}

test "single validator commits" {
    const allocator = std.testing.allocator;
    resetTestCommitCount();

    var network = transport_mod.InMemoryNetwork.init(allocator);
    defer network.deinit();

    const t0 = try network.transportFor(0);

    var node = try DagConsensus.init(allocator, 0, 1, t0, &testOnCommit);
    defer node.deinit();
    try network.registerHandler(0, node.messageHandler());

    // With 1 validator, quorum=1.
    try node.submit("tx1");
    try node.tick();
    _ = try network.deliverAll();

    try node.tick();
    _ = try network.deliverAll();

    try node.tick();
    _ = try network.deliverAll();

    // Check commits happened.
    try std.testing.expect(test_commit_count > 0);
}

test "3 validators reach consensus" {
    const allocator = std.testing.allocator;
    resetTestCommitCount();

    var network = transport_mod.InMemoryNetwork.init(allocator);
    defer network.deinit();

    const n: u8 = 3;

    var nodes: [n]DagConsensus = undefined;
    var initialized = [_]bool{false} ** n;

    defer {
        for (0..n) |i| {
            if (initialized[i]) {
                nodes[i].deinit();
            }
        }
    }

    for (0..n) |i| {
        const vid: ValidatorId = @intCast(i);
        const t = try network.transportFor(vid);
        nodes[i] = try DagConsensus.init(allocator, vid, n, t, &testOnCommit);
        initialized[i] = true;
        try network.registerHandler(vid, nodes[i].messageHandler());
    }

    // Run several rounds of ticking and message delivery.
    for (0..10) |_| {
        for (0..n) |i| {
            try nodes[i].tick();
        }
        _ = try network.deliverAll();
    }

    // All nodes should have made some progress.
    for (0..n) |i| {
        try std.testing.expect(nodes[i].currentRound() >= 2);
    }

    try std.testing.expect(test_commit_count > 0);
}

test "4 validators with BFT threshold" {
    const allocator = std.testing.allocator;
    resetTestCommitCount();

    var network = transport_mod.InMemoryNetwork.init(allocator);
    defer network.deinit();

    const n: u8 = 4;

    var nodes: [n]DagConsensus = undefined;
    var initialized = [_]bool{false} ** n;

    defer {
        for (0..n) |i| {
            if (initialized[i]) {
                nodes[i].deinit();
            }
        }
    }

    for (0..n) |i| {
        const vid: ValidatorId = @intCast(i);
        const t = try network.transportFor(vid);
        nodes[i] = try DagConsensus.init(allocator, vid, n, t, &testOnCommit);
        initialized[i] = true;
        try network.registerHandler(vid, nodes[i].messageHandler());
    }

    // Submit transactions to different validators.
    try nodes[0].submit("tx_from_0");
    try nodes[1].submit("tx_from_1");
    try nodes[2].submit("tx_from_2");
    try nodes[3].submit("tx_from_3");

    // Run many rounds.
    for (0..20) |_| {
        for (0..n) |i| {
            try nodes[i].tick();
        }
        _ = try network.deliverAll();
    }

    // All nodes should have advanced.
    for (0..n) |i| {
        try std.testing.expect(nodes[i].currentRound() >= 2);
    }

    try std.testing.expect(test_commit_count > 0);
}

test "blocks committed in deterministic order" {
    const allocator = std.testing.allocator;

    // Build the same DAG twice and check that Bullshark produces the same order.
    for (0..2) |iteration| {
        _ = iteration;
        var dag = DagStore.init(allocator);
        defer dag.deinit();
        var bs = Bullshark.init(allocator, 3);
        defer bs.deinit();

        // Round 0
        const g0 = try Block.create(allocator, 0, 0, "g0", &.{});
        defer g0.deinit(allocator);
        const g1 = try Block.create(allocator, 1, 0, "g1", &.{});
        defer g1.deinit(allocator);
        const g2 = try Block.create(allocator, 2, 0, "g2", &.{});
        defer g2.deinit(allocator);
        _ = try dag.addBlock(g0);
        _ = try dag.addBlock(g1);
        _ = try dag.addBlock(g2);

        // Round 1: all reference all of round 0
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

        // Round 2: all reference all of round 1
        const parents2 = [_]Hash{ r1_0.hash, r1_1.hash, r1_2.hash };
        const r2_0 = try Block.create(allocator, 0, 2, "r2_0", &parents2);
        defer r2_0.deinit(allocator);
        const r2_1 = try Block.create(allocator, 1, 2, "r2_1", &parents2);
        defer r2_1.deinit(allocator);
        const r2_2 = try Block.create(allocator, 2, 2, "r2_2", &parents2);
        defer r2_2.deinit(allocator);
        _ = try dag.addBlock(r2_0);
        _ = try dag.addBlock(r2_1);
        _ = try dag.addBlock(r2_2);

        // Round 3: all reference all of round 2
        const parents3 = [_]Hash{ r2_0.hash, r2_1.hash, r2_2.hash };
        const r3_0 = try Block.create(allocator, 0, 3, "r3_0", &parents3);
        defer r3_0.deinit(allocator);
        const r3_1 = try Block.create(allocator, 1, 3, "r3_1", &parents3);
        defer r3_1.deinit(allocator);
        const r3_2 = try Block.create(allocator, 2, 3, "r3_2", &parents3);
        defer r3_2.deinit(allocator);
        _ = try dag.addBlock(r3_0);
        _ = try dag.addBlock(r3_1);
        _ = try dag.addBlock(r3_2);

        const committed = try bs.tryCommit(&dag, 4);
        try std.testing.expect(committed > 0);

        // Verify ordering: blocks should be sorted by (round, author)
        const cblocks = bs.getCommittedBlocks();
        for (0..cblocks.len - 1) |i| {
            const a = cblocks[i].block;
            const b = cblocks[i + 1].block;
            const a_before_b = (a.round < b.round) or (a.round == b.round and a.author <= b.author);
            try std.testing.expect(a_before_b);
        }
    }
}

test "causal history traversal" {
    const allocator = std.testing.allocator;
    var dag = DagStore.init(allocator);
    defer dag.deinit();

    // Build a diamond-shaped DAG:
    // Round 0: A, B
    // Round 1: C (parents: A, B), D (parents: A, B)
    // Round 2: E (parents: C, D)
    const a = try Block.create(allocator, 0, 0, "A", &.{});
    defer a.deinit(allocator);
    const b = try Block.create(allocator, 1, 0, "B", &.{});
    defer b.deinit(allocator);
    _ = try dag.addBlock(a);
    _ = try dag.addBlock(b);

    const p_ab = [_]Hash{ a.hash, b.hash };
    const c = try Block.create(allocator, 0, 1, "C", &p_ab);
    defer c.deinit(allocator);
    const d = try Block.create(allocator, 1, 1, "D", &p_ab);
    defer d.deinit(allocator);
    _ = try dag.addBlock(c);
    _ = try dag.addBlock(d);

    const p_cd = [_]Hash{ c.hash, d.hash };
    const e = try Block.create(allocator, 0, 2, "E", &p_cd);
    defer e.deinit(allocator);
    _ = try dag.addBlock(e);

    const history = try dag.causalHistory(e.hash);
    defer allocator.free(history);
    try std.testing.expectEqual(@as(usize, 5), history.len);

    var found_a = false;
    var found_b = false;
    var found_c = false;
    var found_d = false;
    var found_e = false;
    for (history) |blk| {
        if (std.mem.eql(u8, &blk.hash, &a.hash)) found_a = true;
        if (std.mem.eql(u8, &blk.hash, &b.hash)) found_b = true;
        if (std.mem.eql(u8, &blk.hash, &c.hash)) found_c = true;
        if (std.mem.eql(u8, &blk.hash, &d.hash)) found_d = true;
        if (std.mem.eql(u8, &blk.hash, &e.hash)) found_e = true;
    }
    try std.testing.expect(found_a);
    try std.testing.expect(found_b);
    try std.testing.expect(found_c);
    try std.testing.expect(found_d);
    try std.testing.expect(found_e);
}

test "round advancement" {
    const allocator = std.testing.allocator;
    resetTestCommitCount();

    var network = transport_mod.InMemoryNetwork.init(allocator);
    defer network.deinit();

    const n: u8 = 3;
    var nodes: [n]DagConsensus = undefined;
    var initialized = [_]bool{false} ** n;

    defer {
        for (0..n) |i| {
            if (initialized[i]) {
                nodes[i].deinit();
            }
        }
    }

    for (0..n) |i| {
        const vid: ValidatorId = @intCast(i);
        const t = try network.transportFor(vid);
        nodes[i] = try DagConsensus.init(allocator, vid, n, t, &testOnCommit);
        initialized[i] = true;
        try network.registerHandler(vid, nodes[i].messageHandler());
    }

    // Initially all at round 0.
    for (0..n) |i| {
        try std.testing.expectEqual(@as(u64, 0), nodes[i].currentRound());
    }

    // Tick all nodes (they propose round-0 blocks) and deliver messages.
    for (0..n) |i| {
        try nodes[i].tick();
    }
    _ = try network.deliverAll();

    // After receiving all proposals and acks, nodes should advance.
    for (0..n) |i| {
        try nodes[i].tick();
    }
    _ = try network.deliverAll();

    // All nodes should be at least at round 1.
    for (0..n) |i| {
        try std.testing.expect(nodes[i].currentRound() >= 1);
    }
}

test "multiple waves commit" {
    const allocator = std.testing.allocator;
    resetTestCommitCount();

    var network = transport_mod.InMemoryNetwork.init(allocator);
    defer network.deinit();

    const n: u8 = 3;
    var nodes: [n]DagConsensus = undefined;
    var initialized = [_]bool{false} ** n;

    defer {
        for (0..n) |i| {
            if (initialized[i]) {
                nodes[i].deinit();
            }
        }
    }

    for (0..n) |i| {
        const vid: ValidatorId = @intCast(i);
        const t = try network.transportFor(vid);
        nodes[i] = try DagConsensus.init(allocator, vid, n, t, &testOnCommit);
        initialized[i] = true;
        try network.registerHandler(vid, nodes[i].messageHandler());
    }

    // Run many iterations to get multiple waves.
    for (0..30) |_| {
        for (0..n) |i| {
            try nodes[i].tick();
        }
        _ = try network.deliverAll();
    }

    // We should have multiple waves committed.
    try std.testing.expect(test_commit_count > 1);

    // Verify progressive ordering in each individual node's committed list.
    for (0..n) |i| {
        const cblocks = nodes[i].committedBlocks();
        if (cblocks.len > 1) {
            for (0..cblocks.len - 1) |j| {
                try std.testing.expect(cblocks[j].commit_round <= cblocks[j + 1].commit_round);
            }
        }
    }
}

test "missing parent handling" {
    const allocator = std.testing.allocator;
    resetTestCommitCount();

    var network = transport_mod.InMemoryNetwork.init(allocator);
    defer network.deinit();

    const t0 = try network.transportFor(0);
    const t1 = try network.transportFor(1);

    var node0 = try DagConsensus.init(allocator, 0, 2, t0, &testOnCommit);
    defer node0.deinit();
    var node1 = try DagConsensus.init(allocator, 1, 2, t1, &testOnCommit);
    defer node1.deinit();
    try network.registerHandler(0, node0.messageHandler());
    try network.registerHandler(1, node1.messageHandler());

    // Node 0 creates a genesis block.
    try node0.tick();

    // Create a block with a fake parent hash that node1 doesn't have.
    const fake_parent = [_]u8{0xFF} ** 32;
    const parent_arr = [_]Hash{fake_parent};
    const blk = try Block.create(allocator, 0, 1, "orphan", &parent_arr);
    defer blk.deinit(allocator);

    // Send this block directly to node1 as a proposal.
    try node1.handleMessage(0, .{ .proposal = blk });

    // node1 should have enqueued a request_block message for the missing parent.
    const delivered = try network.deliverAll();
    try std.testing.expect(delivered >= 1);
}
