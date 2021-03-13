// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use bls::PublicKeySet;
#[cfg(feature = "simulated-payouts")]
use sn_data_types::Transfer;
use sn_data_types::{
    ActorHistory, Blob, BlobAddress, Credit, CreditAgreementProof, NodeRewardStage, PublicKey,
    ReplicaEvent, SignatureShare, SignedCredit, SignedTransfer, SignedTransferShare,
    TransferAgreementProof, TransferValidated, WalletInfo,
};
use sn_messaging::{client::Message, Aggregation, DstLocation, EndUser, MessageId, SrcLocation};
use sn_routing::Prefix;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter},
};
use xor_name::XorName;

/// Internal messages are what is passed along
/// within a node, between the entry point and
/// exit point of remote messages.
/// In other words, when communication from another
/// participant at the network arrives, it is analysed
/// and interpreted into an internal message, that can
/// then be passed along to its proper processing module
/// at the node. At a node module, the result of such a call
/// is also an internal message.
/// Finally, an internal message might be destined for Messaging
/// module, by which it leaves the process boundary of this node
/// and is sent on the wire to some other destination(s) on the network.

/// Vec of NodeDuty
pub type NodeDuties = Vec<NodeDuty>;

/// Vec of NetworkDuty
pub type NetworkDuties = Vec<NetworkDuty>;

/// All duties carried out by
/// a node in the network.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkDuty {
    RunAsNode(NodeDuty),
    NoOp,
}

// --------------- Node ---------------

/// Common duties run by all nodes.
#[allow(clippy::large_enum_variant)]
pub enum NodeDuty {
    /// Send a message to the specified dst.
    Send(OutgoingMsg),
    /// Send the same request to each individual node.
    SendToNodes {
        targets: BTreeSet<XorName>,
        msg: Message,
    },
    NoOp,
}

impl From<NodeDuty> for NodeDuties {
    fn from(duty: NodeDuty) -> Self {
        if matches!(duty, NodeDuty::NoOp) {
            vec![]
        } else {
            vec![duty]
        }
    }
}

impl Debug for NodeDuty {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoOp => write!(f, "No op."),
            Self::Send(msg) => write!(f, "Send [ msg: {:?} ]", msg),
            Self::SendToNodes { targets, msg } => {
                write!(f, "SendToNodes [ targets: {:?}, msg: {:?} ]", targets, msg)
            }
        }
    }
}

// --------------- Messaging ---------------

#[derive(Debug, Clone)]
pub struct OutgoingMsg {
    pub msg: Message,
    pub dst: DstLocation,
    pub section_source: bool,
    pub aggregation: Aggregation,
}

impl OutgoingMsg {
    pub fn id(&self) -> MessageId {
        self.msg.id()
    }
}

/// This duty is at the border of infrastructural
/// and domain duties. Messaging is such a fundamental
/// part of the system, that it can be considered domain.
#[allow(clippy::large_enum_variant)]
pub enum NodeMessagingDuty {
    // No operation
    NoOp,
}

impl Debug for NodeMessagingDuty {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoOp => write!(f, "No op."),
        }
    }
}

/// Queries for chunk to replicate
#[derive(Debug)]
pub enum ChunkReplicationQuery {
    ///
    GetChunk(BlobAddress),
}

/// Cmds carried out on Adults.
#[derive(Debug)]
#[allow(clippy::clippy::large_enum_variant)]
pub enum ChunkReplicationCmd {
    /// An imperament to retrieve
    /// a chunk from current holders, in order
    /// to replicate it locally.
    ReplicateChunk {
        ///
        current_holders: BTreeSet<XorName>,
        ///
        address: BlobAddress,
        // ///
        // section_authority: MsgSender,
    },
    StoreReplicatedBlob(Blob),
}

// --------------- Rewards ---------------

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum RewardCmd {
    /// Initiates a new SectionActor with the
    /// state of existing Replicas in the group.
    SynchHistory(WalletInfo),
    /// Completes transition to a new SectionActor, i.e. new wallet,
    /// and sets it up with its replicas' public key set.
    CompleteWalletTransition(PublicKeySet),
    /// With the node id.
    AddNewNode(XorName),
    /// Set the account for a node.
    SetNodeWallet {
        /// The node which accumulated the rewards.
        node_id: XorName,
        /// The account to which the accumulated
        /// rewards should be paid out.
        wallet_id: PublicKey,
    },
    /// We add relocated nodes to our rewards
    /// system, so that they can participate
    /// in the farming rewards.
    AddRelocatingNode {
        /// The id of the node at the previous section.
        old_node_id: XorName,
        /// The id of the node at its new section (i.e. this one).
        new_node_id: XorName,
        // The age of the node, determines if it is eligible for rewards yet.
        age: u8,
    },
    /// When a node has been relocated to our section
    /// we receive the account id from the other section.
    ActivateNodeRewards {
        /// The account to which the accumulated
        /// rewards should be paid out.
        id: PublicKey,
        /// The node which accumulated the rewards.
        node_id: XorName,
    },
    /// When a node has left for some reason,
    /// we deactivate it.
    DeactivateNode(XorName),
    /// The distributed Actor of a section,
    /// receives and accumulates the validated
    /// reward payout from its Replicas,
    ReceivePayoutValidation(TransferValidated),
}

/// payouts from the section account.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum RewardQuery {
    /// When a node is relocated from us, the other
    /// section will query for the node wallet id.
    GetNodeWalletId {
        /// The id of the node at the previous section.
        old_node_id: XorName,
        /// The id of the node at its new section (i.e. this one).
        new_node_id: XorName,
    },
    // /// When a new Section Actor share joins,
    // /// it queries the other shares for the section wallet history.
    // GetSectionWalletHistory,
}

/// Queries for information on accounts,
/// handled by AT2 Replicas.
#[derive(Debug)]
pub enum TransferQuery {
    /// Get the PublicKeySet for replicas of a given PK
    GetReplicaKeys(PublicKey),
    /// Get key balance.
    GetBalance(PublicKey),
    /// Get key transfers since specified version.
    GetHistory {
        /// The wallet key.
        at: PublicKey,
        /// The last version of transfers we know of.
        since_version: usize,
    },
    GetReplicaEvents,
    /// Get the latest cost for writing given number of bytes to network.
    GetStoreCost {
        /// The requester's key.
        requester: PublicKey,
        /// Number of bytes to write.
        bytes: u64,
    },
}

/// Cmds carried out on AT2 Replicas.
#[derive(Debug)]
#[allow(clippy::clippy::large_enum_variant)]
pub enum TransferCmd {
    /// Initiates a new Replica with the
    /// state of existing Replicas in the group.
    InitiateReplica(Vec<ReplicaEvent>),
    ProcessPayment(Message),
    #[cfg(feature = "simulated-payouts")]
    /// Cmd to simulate a farming payout
    SimulatePayout(Transfer),
    /// The cmd to validate a transfer.
    ValidateTransfer(SignedTransfer),
    /// The cmd to register the consensused transfer.
    RegisterTransfer(TransferAgreementProof),
    /// As a transfer has been propagated to the
    /// crediting section, it is applied there.
    PropagateTransfer(CreditAgreementProof),
    /// The validation of a section transfer.
    ValidateSectionPayout(SignedTransferShare),
    /// The registration of a section transfer.
    RegisterSectionPayout(TransferAgreementProof),
}

impl From<sn_messaging::client::TransferCmd> for TransferCmd {
    fn from(cmd: sn_messaging::client::TransferCmd) -> Self {
        match cmd {
            #[cfg(feature = "simulated-payouts")]
            sn_messaging::client::TransferCmd::SimulatePayout(transfer) => {
                Self::SimulatePayout(transfer)
            }
            sn_messaging::client::TransferCmd::ValidateTransfer(signed_transfer) => {
                Self::ValidateTransfer(signed_transfer)
            }
            sn_messaging::client::TransferCmd::RegisterTransfer(transfer_agreement) => {
                Self::RegisterTransfer(transfer_agreement)
            }
        }
    }
}

impl From<sn_messaging::client::TransferQuery> for TransferQuery {
    fn from(cmd: sn_messaging::client::TransferQuery) -> Self {
        match cmd {
            sn_messaging::client::TransferQuery::GetReplicaKeys(transfer) => {
                Self::GetReplicaKeys(transfer)
            }
            sn_messaging::client::TransferQuery::GetBalance(public_key) => {
                Self::GetBalance(public_key)
            }
            sn_messaging::client::TransferQuery::GetHistory { at, since_version } => {
                Self::GetHistory { at, since_version }
            }
            sn_messaging::client::TransferQuery::GetStoreCost { requester, bytes } => {
                Self::GetStoreCost { requester, bytes }
            }
        }
    }
}
