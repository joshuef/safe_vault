// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::collections::BTreeMap;

use super::genesis_stage::{GenesisAccumulation, GenesisProposal, GenesisStage};
use crate::{
    capacity::{Capacity, ChunkHolderDbs, RateLimit},
    node::{
        messaging::send,
        node_ops::OutgoingMsg,
        transfers::{
            replica_signing::ReplicaSigningImpl,
            replicas::{ReplicaInfo, Replicas},
            Transfers,
        },
    },
    Error, Network, Node, Result, Storage,
};
use log::{debug, info, trace, warn};
use sn_data_types::{
    ActorHistory, Credit, NodeRewardStage, PublicKey, SignatureShare, SignedCredit, Token,
    TransferPropagated,
};
use sn_messaging::{
    client::{Message, NodeCmd, NodeSystemCmd},
    Aggregation, DstLocation, MessageId,
};
use sn_routing::{XorName, ELDER_SIZE as GENESIS_ELDER_COUNT};

impl Storage {
    /// start genesis section formation. ProposeGenesis if there are enough nodes.
    pub async fn begin_forming_genesis_section(
        &self,
        network: Network,
        mut storage: Storage,
    ) -> Result<()> {
        let is_genesis_section = network.our_prefix().await.is_empty();
        let elder_count = network.our_elder_names().await.len();
        let section_chain_len = network.section_chain().await.len();
        let our_pk_share = network.our_public_key_share().await?;
        let our_index = network.our_index().await?;

        debug!(
            "begin_transition_to_elder. is_genesis_section: {}, elder_count: {}, section_chain_len: {}",
            is_genesis_section, elder_count, section_chain_len
        );
        if is_genesis_section
            && elder_count == GENESIS_ELDER_COUNT
            && section_chain_len <= GENESIS_ELDER_COUNT
        {
            // this is the case when we are the GENESIS_ELDER_COUNT-th Elder!
            debug!("threshold reached; proposing genesis!");

            let genesis_balance = u32::MAX as u64 * 1_000_000_000;
            let credit = Credit {
                id: Default::default(),
                amount: Token::from_nano(genesis_balance),
                recipient: network
                    .section_public_key()
                    .await
                    .ok_or(Error::NoSectionPublicKey)?,
                msg: "genesis".to_string(),
            };
            let mut signatures: BTreeMap<usize, bls::SignatureShare> = Default::default();
            let credit_sig_share = network.sign_as_elder(&credit).await?;
            let _ = signatures.insert(our_index, credit_sig_share.clone());

            let msg = OutgoingMsg {
                msg: Message::NodeCmd {
                    cmd: NodeCmd::System(NodeSystemCmd::ProposeGenesis {
                        credit: credit.clone(),
                        sig: SignatureShare {
                            share: credit_sig_share,
                            index: our_index,
                        },
                    }),
                    id: MessageId::new(),
                    target_section_pk: None,
                },
                dst: DstLocation::Section(credit.recipient.into()),
                section_source: false, // sent as single node
                aggregation: Aggregation::None,
            };

            send(msg, network).await?;

            *storage.genesis_stage.lock().await = GenesisStage::ProposingGenesis(GenesisProposal {
                proposal: credit.clone(),
                signatures,
                pending_agreement: None,
            });

            Ok(())
        } else if is_genesis_section
            && elder_count < GENESIS_ELDER_COUNT
            && section_chain_len <= GENESIS_ELDER_COUNT
        {
            debug!("AwaitingGenesisThreshold!");
            *storage.genesis_stage.lock().await = GenesisStage::AwaitingGenesisThreshold;
            Ok(())
        } else {
            Err(Error::InvalidOperation(
                "Only for genesis formation".to_string(),
            ))
        }
    }

    // TODO: validate the credit...
    /// Handle receipt of genesis proposal
    pub async fn receive_genesis_proposal(
        &self,
        credit: Credit,
        sig: SignatureShare,
        mut storage: Storage,
        // stage: GenesisStage,
        network: Network,
    ) -> Result<()> {
        trace!("Handling received proposal");
        let our_index = network.our_index().await?;
        let mut stage = GenesisStage::None;
        {
            stage = storage.genesis_stage.lock().await.clone();
        }

        match stage {
            GenesisStage::AwaitingGenesisThreshold => {
                info!("We were awaiting genesis threshold......");
                let mut signatures: BTreeMap<usize, bls::SignatureShare> = Default::default();
                let _ = signatures.insert(sig.index, sig.share);

                let credit_sig_share = network.sign_as_elder(&credit).await?;
                let _ = signatures.insert(our_index, credit_sig_share.clone());

                let dst = DstLocation::Section(XorName::from(
                    network
                        .section_public_key()
                        .await
                        .ok_or(Error::NoSectionPublicKey)?,
                ));

                let msg = OutgoingMsg {
                    msg: Message::NodeCmd {
                        cmd: NodeCmd::System(NodeSystemCmd::ProposeGenesis {
                            credit: credit.clone(),
                            sig: SignatureShare {
                                share: credit_sig_share,
                                index: our_index,
                            },
                        }),
                        id: MessageId::new(),
                        target_section_pk: None,
                    },
                    section_source: false, // sent as single node
                    dst,
                    aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
                };

                send(msg, network).await?;

                *storage.genesis_stage.lock().await =
                    GenesisStage::ProposingGenesis(GenesisProposal {
                        proposal: credit.clone(),
                        signatures,
                        pending_agreement: None,
                    });
                Ok(())
            }
            GenesisStage::ProposingGenesis(mut bootstrap) => {
                info!("Adding incoming genesis proposal.");
                let section_pk_set = network
                    .our_public_key_set()
                    .await
                    .map_err(|_| Error::NoSectionPublicKeySet)?;
                let section_pk = PublicKey::Bls(section_pk_set.public_key());
                bootstrap.add(sig, section_pk_set)?;
                if let Some(signed_credit) = &bootstrap.pending_agreement {
                    debug!("there is an agreement");
                    // replicas signatures over > signed_credit <
                    let mut signatures: BTreeMap<usize, bls::SignatureShare> = Default::default();
                    let credit_sig_share = network.sign_as_elder(&signed_credit).await?;
                    let _ = signatures.insert(our_index, credit_sig_share.clone());

                    let msg = OutgoingMsg {
                        msg: Message::NodeCmd {
                            cmd: NodeCmd::System(NodeSystemCmd::AccumulateGenesis {
                                signed_credit: signed_credit.clone(),
                                sig: SignatureShare {
                                    share: credit_sig_share,
                                    index: our_index,
                                },
                            }),
                            id: MessageId::new(),
                            target_section_pk: None,
                        },
                        section_source: false, // sent as single node
                        dst: DstLocation::Section(XorName::from(section_pk)),
                        aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
                    };

                    send(msg, network).await?;

                    *storage.genesis_stage.lock().await =
                        GenesisStage::AccumulatingGenesis(GenesisAccumulation {
                            agreed_proposal: signed_credit.clone(),
                            signatures,
                            pending_agreement: None,
                        });

                    Ok(())
                } else {
                    debug!("Another stage... setting to proposing");
                    *storage.genesis_stage.lock().await = GenesisStage::ProposingGenesis(bootstrap);

                    Ok(())
                }
            }
            _ => {
                warn!("Recevied an out of order proposal for genesis.");
                warn!(
                    "We may already have seen + verified genesis, in which case this can be ignored."
                );

                // TODO: do we want to Lazy err here?

                Ok(())
            }
        }
    }

    /// Receive genesis accumulation
    pub async fn receive_genesis_accumulation(
        &self,
        signed_credit: SignedCredit,
        sig: SignatureShare,
        // stage: GenesisStage,
        network: Network,
        mut storage: Storage,
    ) -> Result<()> {
        trace!("Receiving genesis accumulation");
        let mut stage = GenesisStage::None;
        {
            // blocked off to avoid deadlock
            stage = storage.genesis_stage.lock().await.clone();
        }
        trace!("Receiving genesis accumulation2222");

        match stage {
            GenesisStage::ProposingGenesis(_) => {
                debug!("we're still proposing...");
                // replicas signatures over > signed_credit <
                let mut signatures: BTreeMap<usize, bls::SignatureShare> = Default::default();
                let _ = signatures.insert(sig.index, sig.share);
                let our_sig_index = network.our_index().await?;

                let credit_sig_share = network.sign_as_elder(&signed_credit).await?;
                let _ = signatures.insert(our_sig_index, credit_sig_share);

                *storage.genesis_stage.lock().await =
                    GenesisStage::AccumulatingGenesis(GenesisAccumulation {
                        agreed_proposal: signed_credit,
                        signatures,
                        pending_agreement: None,
                    });

                Ok(())
            }
            GenesisStage::AccumulatingGenesis(mut bootstrap) => {
                debug!("were accumulating!...++++++");

                let section_pk_set = network
                    .our_public_key_set()
                    .await
                    .map_err(|_| Error::NoSectionPublicKeySet)?;
                bootstrap.add(sig, section_pk_set)?;

                if let Some(genesis) = bootstrap.pending_agreement.take() {
                    debug!("We have a prooof!!!!!!!!");
                    // TODO: do not take this? (in case of fail further blow)
                    let our_sig_index = network.our_index().await?;
                    let credit_sig_share = network.sign_as_elder(&genesis).await?;
                    let _ = bootstrap
                        .signatures
                        .insert(our_sig_index, credit_sig_share.clone());

                    *storage.genesis_stage.lock().await =
                        GenesisStage::Completed(TransferPropagated {
                            credit_proof: genesis.clone(),
                        });

                    // self.complete_elder_setup(storage, genesis_tx, network)

                    Ok(())
                } else {
                    *storage.genesis_stage.lock().await =
                        GenesisStage::AccumulatingGenesis(bootstrap);

                    Ok(())
                }
            }
            _ => Err(Error::InvalidGenesisStage),
        }
    }

    async fn register_wallet(&self, reward_key: PublicKey, network: Network) -> Result<()> {
        let address = network.our_prefix().await.name();
        info!("Registering wallet: {}", reward_key);
        let msg = OutgoingMsg {
            msg: Message::NodeCmd {
                cmd: NodeCmd::System(NodeSystemCmd::RegisterWallet(reward_key)),
                id: MessageId::new(),
                target_section_pk: None,
            },
            section_source: false, // sent as single node
            dst: DstLocation::Section(address),
            aggregation: Aggregation::None,
        };
        send(msg, network).await
    }

    async fn create_transfer_replicas(
        &self,
        node_info: &Storage,
        network: Network,
        user_wallets: BTreeMap<PublicKey, ActorHistory>,
    ) -> Result<Replicas<ReplicaSigningImpl>> {
        let root_dir = node_info.root_dir.clone();
        let id = network
            .our_public_key_share()
            .await?
            .bls_share()
            .ok_or(Error::ProvidedPkIsNotBlsShare)?;
        let key_index = network.our_index().await?;
        let peer_replicas = network.our_public_key_set().await?.clone();
        let signing = ReplicaSigningImpl::new(network.clone());
        let info = ReplicaInfo {
            id,
            key_index,
            peer_replicas,
            section_chain: network.section_chain().await.clone(),
            signing,
            initiating: true,
        };
        Replicas::new(root_dir, info, user_wallets).await
    }
}
