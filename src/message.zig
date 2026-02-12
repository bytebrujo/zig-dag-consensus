const types = @import("types.zig");
const block_mod = @import("block.zig");
const ValidatorId = types.ValidatorId;
const Hash = types.Hash;
const Block = block_mod.Block;

pub const Message = union(enum) {
    /// Propose a new block to the network.
    proposal: Block,
    /// Acknowledge receipt of a block.
    ack: Ack,
    /// Request a missing block by hash.
    request_block: Hash,
    /// Response with block data.
    block_response: Block,

    pub const Ack = struct {
        block_hash: Hash,
        from: ValidatorId,
    };
};
