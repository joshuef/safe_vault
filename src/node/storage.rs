// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

// mod handle_msg;
// mod handle_network_event;
// mod messaging;
// mod metadata;
// mod section_funds;
// mod transfers;
// mod work;

// pub(crate) mod node_ops;
// pub mod state_db;

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

use crate::node::{
    genesis::genesis_stage::GenesisStage,
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

/// Info about the node.
#[derive(Clone)]
pub struct Storage {
    ///
    pub genesis: bool,
    ///
    pub root_dir: PathBuf,
    ///
    pub used_space: UsedSpace,
    /// The key used by the node to receive earned rewards.
    // TODO: Arc
    pub reward_key: PublicKey,

    // prefix: Option<Prefix>,
    // node_name: XorName,
    // node_id: Ed25519PublicKey,
    pub(crate) genesis_stage: Arc<Mutex<GenesisStage>>,

    pub(crate) network: Network,

    // TODO: ensure cloning of these has no adverse affects. Are we Arc all over?

    // data operations
    pub(crate) meta_data: Arc<Mutex<Option<Metadata>>>,
    // transfers
    pub(crate) transfers: Arc<Mutex<Option<Transfers>>>,
    // reward payouts
    pub(crate) section_funds: Arc<Mutex<Option<SectionFunds>>>,
}

impl Storage {
    ///
    pub fn path(&self) -> &Path {
        self.root_dir.as_path()
    }

    /// Remove database storage and other actions associated with a demotion.
    pub async fn level_down(&self) {
        *self.meta_data.lock().await = None;
        *self.transfers.lock().await = None;
        *self.section_funds.lock().await = None;
    }

    /// Set up databases ready for eldership
    pub async fn level_up(&self, genesis_tx: Option<TransferPropagated>) -> Result<()> {
        info!(
            "Levelling up storage. With a genesis tx?: {:?}",
            genesis_tx.is_some()
        );
        // metadata
        let dbs = ChunkHolderDbs::new(self.path())?;
        let reader = SomethingThatShouldBeQueriedFromRoutingAdultReader::new(self.network.clone());
        let meta_data = Metadata::new(&self, dbs, reader).await?;
        *self.meta_data.lock().await = Some(meta_data);

        // transfers
        let dbs = ChunkHolderDbs::new(self.root_dir.as_path())?;
        let rate_limit = RateLimit::new(self.network.clone(), Capacity::new(dbs.clone()));
        let user_wallets = BTreeMap::<PublicKey, ActorHistory>::new();
        let replicas = transfer_replicas(&self, self.network.clone(), user_wallets).await?;
        let transfers = Transfers::new(replicas, rate_limit);
        // does local init, with no roundrip via network messaging
        if let Some(genesis_tx) = genesis_tx {
            transfers.genesis(genesis_tx.clone()).await?;
        }
        *self.transfers.lock().await = Some(transfers);

        // rewards
        // let actor = TransferActor::from_info(signing, reward_data.section_wallet, Validator {})?;
        // let wallet = RewardingWallet::new(actor);
        // self.section_funds = Some(SectionFunds::Rewarding(wallet));
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

impl Display for Storage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "NodeOperation")
    }
}
