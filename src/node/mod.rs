// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod genesis;
mod handle_msg;
mod handle_network_event;
mod messaging;
mod metadata;
mod section_funds;
pub mod storage;
mod transfers;

pub(crate) mod node_ops;
pub mod state_db;

use crate::{
    capacity::{Capacity, ChunkHolderDbs, RateLimit},
    chunk_store::UsedSpace,
    Config, Error, Network, Result,
};
use bls::SecretKey;
use ed25519_dalek::PublicKey as Ed25519PublicKey;
use futures::lock::Mutex;
use hex_fmt::HexFmt;
use log::{debug, error, info, trace};
use sn_data_types::{ActorHistory, NodeRewardStage, PublicKey, TransferPropagated, WalletInfo};
use sn_messaging::{client::Message, DstLocation, SrcLocation};
use sn_routing::{Event as RoutingEvent, EventStream, NodeElderChange, MIN_AGE};
use sn_routing::{Prefix, XorName, ELDER_SIZE as GENESIS_ELDER_COUNT};
use sn_transfers::{TransferActor, Wallet};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};

use self::{
    genesis::genesis_stage::GenesisStage,
    // storage::Storage,
    handle_msg::handle,
    handle_network_event::handle_network_event,
    messaging::send,
    messaging::send_to_nodes,
    metadata::{adult_reader::SomethingThatShouldBeQueriedFromRoutingAdultReader, Metadata},
    node_ops::{NodeDuties, NodeDuty},
    section_funds::{rewarding_wallet::RewardingWallet, SectionFunds},
    state_db::store_new_reward_keypair,
    transfers::get_replicas::transfer_replicas,
    transfers::Transfers,
};

pub use storage::Storage;
/// Main node struct.
pub struct Node {
    network: Network,
    network_events: EventStream,
    storage: Storage,
}

impl Node {
    /// Initialize a new node.
    pub async fn new(config: &Config) -> Result<Self> {
        // TODO: STARTUP all things
        let root_dir_buf = config.root_dir()?;
        let root_dir = root_dir_buf.as_path();
        std::fs::create_dir_all(root_dir)?;

        debug!("NEW NODE");
        let reward_key_task = async move {
            let res: Result<PublicKey>;
            match config.wallet_id() {
                Some(public_key) => {
                    res = Ok(PublicKey::Bls(state_db::pk_from_hex(public_key)?));
                }
                None => {
                    let secret = SecretKey::random();
                    let public = secret.public_key();
                    store_new_reward_keypair(root_dir, &secret, &public).await?;
                    res = Ok(PublicKey::Bls(public));
                }
            };
            res
        }
        .await;

        let reward_key = reward_key_task?;
        debug!("NEW NODE after reward key");
        let (network, network_events) = Network::new(config).await?;

        // TODO: This should be general setup tbh..

        let storage = Storage {
            genesis: config.is_first(),
            root_dir: root_dir_buf,
            used_space: UsedSpace::new(config.max_capacity()),
            reward_key,
            genesis_stage: Arc::new(Mutex::new(GenesisStage::None)),
            meta_data: Arc::new(Mutex::new(None)),
            transfers: Arc::new(Mutex::new(None)),
            section_funds: Arc::new(Mutex::new(None)),

            network: network.clone(),
        };

        debug!("NEW NODE after messaging");

        let node = Self {
            storage,
            network,
            network_events,
        };

        Ok(node)
    }

    /// Returns our connection info.
    pub async fn our_connection_info(&mut self) -> SocketAddr {
        self.network.our_connection_info().await
    }

    /// Returns whether routing node is in elder state.
    pub async fn is_elder(&self) -> bool {
        self.network.is_elder().await
    }

    /// Starts the node, and runs the main event loop.
    /// Blocks until the node is terminated, which is done
    /// by client sending in a `Command` to free it.
    pub async fn run(mut self) -> Result<()> {
        // TODO: setup all the bits we need here:

        let info = self.network.our_connection_info().await;
        info!("Listening for routing events at: {}", info);
        while let Some(event) = self.network_events.next().await {
            let join_handle = tokio::spawn(
                handle_network_event(event, self.network.clone(), self.storage.clone()), // self.process_while_any(node_duties).await;
            );
        }

        Ok(())
    }

    fn handle_error(&self, err: &Error) {
        use std::error::Error;
        info!(
            "unimplemented: Handle errors. This should be return w/ lazyError to sender. {}",
            err
        );

        if let Some(source) = err.source() {
            error!("Source of error: {:?}", source);
        }
    }
}

impl Display for Node {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Node")
    }
}
