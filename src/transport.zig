const std = @import("std");
const types = @import("types.zig");
const message_mod = @import("message.zig");
const ValidatorId = types.ValidatorId;
const Message = message_mod.Message;
const Block = @import("block.zig").Block;
const Hash = types.Hash;

pub const Transport = struct {
    send_fn: *const fn (ctx: *anyopaque, target: ValidatorId, msg: Message) void,
    ctx: *anyopaque,

    pub fn send(self: Transport, target: ValidatorId, msg: Message) void {
        self.send_fn(self.ctx, target, msg);
    }
};

/// Callback type for delivering messages to a consensus instance.
pub const MessageHandler = struct {
    handle_fn: *const fn (ctx: *anyopaque, from: ValidatorId, msg: Message) void,
    ctx: *anyopaque,

    pub fn handle(self: MessageHandler, from: ValidatorId, msg: Message) void {
        self.handle_fn(self.ctx, from, msg);
    }
};

/// In-memory transport for testing. Queues messages and delivers them on demand.
pub const InMemoryNetwork = struct {
    const QueuedMessage = struct {
        from: ValidatorId,
        to: ValidatorId,
        msg_type: MsgType,
        // Stored data for the message
        block_author: ValidatorId = 0,
        block_round: u64 = 0,
        block_payload: []const u8 = &.{},
        block_parents: []const Hash = &.{},
        block_hash: Hash = types.ZERO_HASH,
        ack_hash: Hash = types.ZERO_HASH,
        ack_from: ValidatorId = 0,
        request_hash: Hash = types.ZERO_HASH,
    };

    const MsgType = enum {
        proposal,
        ack,
        request_block,
        block_response,
    };

    allocator: std.mem.Allocator,
    queue: std.ArrayList(QueuedMessage),
    handlers: std.AutoArrayHashMap(ValidatorId, MessageHandler),
    node_transports: std.AutoArrayHashMap(ValidatorId, NodeTransport),

    pub fn init(allocator: std.mem.Allocator) InMemoryNetwork {
        return .{
            .allocator = allocator,
            .queue = .empty,
            .handlers = std.AutoArrayHashMap(ValidatorId, MessageHandler).init(allocator),
            .node_transports = std.AutoArrayHashMap(ValidatorId, NodeTransport).init(allocator),
        };
    }

    pub fn deinit(self: *InMemoryNetwork) void {
        // Free all stored payloads and parents in the queue.
        for (self.queue.items) |item| {
            if (item.block_payload.len > 0) {
                self.allocator.free(item.block_payload);
            }
            if (item.block_parents.len > 0) {
                self.allocator.free(item.block_parents);
            }
        }
        self.queue.deinit(self.allocator);
        self.handlers.deinit();
        self.node_transports.deinit();
    }

    pub fn registerHandler(self: *InMemoryNetwork, id: ValidatorId, handler: MessageHandler) !void {
        try self.handlers.put(id, handler);
    }

    /// Create a Transport for the given validator ID that routes through this network.
    pub fn transportFor(self: *InMemoryNetwork, node_id: ValidatorId) !Transport {
        const gop = try self.node_transports.getOrPut(node_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = NodeTransport{ .network = self, .node_id = node_id };
        }
        return Transport{
            .send_fn = @ptrCast(&NodeTransport.sendMessage),
            .ctx = @ptrCast(gop.value_ptr),
        };
    }

    /// Deliver all queued messages to their target handlers.
    /// Returns the number of messages delivered.
    pub fn deliverAll(self: *InMemoryNetwork) !u32 {
        var delivered: u32 = 0;
        while (self.queue.items.len > 0) {
            // Take the current batch and clear the queue so newly enqueued messages
            // during delivery are handled in the next iteration.
            var batch: std.ArrayList(QueuedMessage) = .empty;
            defer batch.deinit(self.allocator);
            std.mem.swap(std.ArrayList(QueuedMessage), &self.queue, &batch);

            for (batch.items) |item| {
                defer {
                    if (item.block_payload.len > 0) {
                        self.allocator.free(item.block_payload);
                    }
                    if (item.block_parents.len > 0) {
                        self.allocator.free(item.block_parents);
                    }
                }
                if (self.handlers.get(item.to)) |handler| {
                    const msg: Message = switch (item.msg_type) {
                        .proposal => .{ .proposal = .{
                            .author = item.block_author,
                            .round = item.block_round,
                            .payload = item.block_payload,
                            .parents = item.block_parents,
                            .hash = item.block_hash,
                        } },
                        .ack => .{ .ack = .{
                            .block_hash = item.ack_hash,
                            .from = item.ack_from,
                        } },
                        .request_block => .{ .request_block = item.request_hash },
                        .block_response => .{ .block_response = .{
                            .author = item.block_author,
                            .round = item.block_round,
                            .payload = item.block_payload,
                            .parents = item.block_parents,
                            .hash = item.block_hash,
                        } },
                    };
                    handler.handle(item.from, msg);
                    delivered += 1;
                }
            }
        }
        return delivered;
    }

    const NodeTransport = struct {
        network: *InMemoryNetwork,
        node_id: ValidatorId,

        fn sendMessage(self: *NodeTransport, target: ValidatorId, msg: Message) void {
            const queued = switch (msg) {
                .proposal => |blk| QueuedMessage{
                    .from = self.node_id,
                    .to = target,
                    .msg_type = .proposal,
                    .block_author = blk.author,
                    .block_round = blk.round,
                    .block_payload = self.network.allocator.dupe(u8, blk.payload) catch return,
                    .block_parents = self.network.allocator.dupe(Hash, blk.parents) catch return,
                    .block_hash = blk.hash,
                },
                .ack => |a| QueuedMessage{
                    .from = self.node_id,
                    .to = target,
                    .msg_type = .ack,
                    .ack_hash = a.block_hash,
                    .ack_from = a.from,
                },
                .request_block => |h| QueuedMessage{
                    .from = self.node_id,
                    .to = target,
                    .msg_type = .request_block,
                    .request_hash = h,
                },
                .block_response => |blk| QueuedMessage{
                    .from = self.node_id,
                    .to = target,
                    .msg_type = .block_response,
                    .block_author = blk.author,
                    .block_round = blk.round,
                    .block_payload = self.network.allocator.dupe(u8, blk.payload) catch return,
                    .block_parents = self.network.allocator.dupe(Hash, blk.parents) catch return,
                    .block_hash = blk.hash,
                },
            };
            self.network.queue.append(self.network.allocator, queued) catch return;
        }
    };
};

test "in-memory transport basic" {
    const allocator = std.testing.allocator;
    var network = InMemoryNetwork.init(allocator);
    defer network.deinit();

    const t0 = try network.transportFor(0);
    t0.send(1, .{ .ack = .{ .block_hash = types.ZERO_HASH, .from = 0 } });

    // No handler registered for 1, so deliverAll should just drop it.
    const delivered = try network.deliverAll();
    try std.testing.expectEqual(@as(u32, 0), delivered);
}
