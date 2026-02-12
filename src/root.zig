//! zig-dag-consensus: A DAG-based consensus protocol implementation
//! inspired by Narwhal/Bullshark for high-TPS blockchains.
//!
//! This library separates data availability (Narwhal-style DAG construction)
//! from ordering (Bullshark commit rules) to achieve high throughput.

pub const types = @import("types.zig");
pub const block = @import("block.zig");
pub const dag = @import("dag.zig");
pub const bullshark = @import("bullshark.zig");
pub const message = @import("message.zig");
pub const transport = @import("transport.zig");
pub const consensus = @import("consensus.zig");

// Re-export primary types for convenience.
pub const ValidatorId = types.ValidatorId;
pub const ValidatorSet = types.ValidatorSet;
pub const Hash = types.Hash;
pub const Block = block.Block;
pub const CommittedBlock = block.CommittedBlock;
pub const DagStore = dag.DagStore;
pub const Bullshark = bullshark.Bullshark;
pub const Message = message.Message;
pub const Transport = transport.Transport;
pub const InMemoryNetwork = transport.InMemoryNetwork;
pub const DagConsensus = consensus.DagConsensus;

test {
    // Pull in all tests from submodules.
    @import("std").testing.refAllDecls(@This());
}
