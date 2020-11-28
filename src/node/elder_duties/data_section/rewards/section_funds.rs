// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::validator::Validator;
use crate::{
    node::msg_wrapping::ElderMsgWrapping,
    node::node_ops::{NodeMessagingDuty, NodeOperation},
};
use crate::{Error, Outcome, TernaryResult};
use sn_data_types::{
    DebitAgreementProof, Message, MessageId, Money, NodeCmd, NodeTransferCmd, PublicKey,
    ReplicaEvent, Result, TransferValidated,
};
use xor_name::XorName;

use log::{debug, error};
use sn_transfers::{ActorEvent, TransferActor};
use std::collections::{BTreeSet, VecDeque};
use ActorEvent::*;
/// The management of section funds,
/// via the usage of a distributed AT2 Actor.
pub(super) struct SectionFunds {
    actor: TransferActor<Validator>,
    wrapping: ElderMsgWrapping,
    state: State,
}

#[derive(Clone)]
pub struct Payout {
    pub to: PublicKey,
    pub amount: Money,
    pub node_id: XorName,
}

struct State {
    /// Incoming payout requests are queued here.
    /// It is queued when we already have a payout in flight,
    /// or when we are transitioning to a new Actor.
    queued_payouts: VecDeque<Payout>,
    payout_in_flight: Option<Payout>,
    finished: BTreeSet<XorName>, // this set grows within acceptable bounds, since transitions do not happen that often, and at every section split, the set is cleared..
    /// While awaiting payout completion
    next_actor: Option<TransferActor<Validator>>, // we could do a queue here, and when starting transition skip all but the last one, but that is also prone to edge case problems..
}

impl SectionFunds {
    pub fn new(actor: TransferActor<Validator>, wrapping: ElderMsgWrapping) -> Self {
        Self {
            actor,
            wrapping,
            state: State {
                queued_payouts: Default::default(),
                payout_in_flight: None,
                finished: Default::default(),
                next_actor: None,
            },
        }
    }

    /// Replica events get synched to section actor instances.
    pub async fn synch(&mut self, events: Vec<ReplicaEvent>) -> Outcome<NodeMessagingDuty> {
        debug!("Synching replica events to section transfer actor...");
        match self.actor.synch(events) {
            Ok(_) => Ok(None),
            Err(e) => Outcome::error(Error::NetworkData(e)), // temp, will be removed with the other panics here shortly
        }
    }

    /// At Elder churn, we must transition to a new account.
    pub async fn transition(&mut self, to: TransferActor<Validator>) -> Outcome<NodeMessagingDuty> {
        debug!("Transitioning section transfer actor...");
        if self.is_transitioning() {
            debug!("is_transitioning");
            // hm, could be tricky edge cases here, but
            // we'll start by assuming there will only be
            // one transition at a time.
            // (We could enqueue actors, and when starting transition skip
            // all but the last one, but that is also prone to edge case problems..)
            return Outcome::error(Error::Logic);
        }

        let new_id = to.id();
        self.state.next_actor = Some(to);
        // When we have a payout in flight, we defer the transition.
        if self.has_payout_in_flight() {
            debug!("has_payout_in_flight");
            return Outcome::error(Error::Logic);
        }

        // Get all the money of current actor.
        let amount = self.actor.balance();
        if amount == Money::zero() {
            debug!("No money to transfer in this section.");
            // if zero, then there is nothing to transfer..
            // so just go ahead and become the new actor.
            return match self.state.next_actor.take() {
                Some(actor) => {
                    self.actor = actor;
                    Ok(None)
                }
                None => {
                    error!("Tried to take next actor while non existed!");
                    Outcome::error(Error::Logic)
                }
            };
        }

        // Transfer the money from
        // previous actor to new actor.
        use NodeCmd::*;
        use NodeTransferCmd::*;
        match self.actor.transfer(amount, new_id) {
            Ok(Some(event)) => {
                match self.apply(TransferInitiated(event.clone())) {
                    Err(e) => {
                        // This would be a bug!
                        // send some error, log, crash .. or something
                        error!("Applying TransferInitiated during transition failed!");
                        Outcome::error(Error::NetworkData(e))
                    }
                    Ok(_) => {
                        debug!(
                            "Section actor transition transfer is being requested of the replicas.."
                        );
                        // We ask of our Replicas to validate this transfer.
                        self.wrapping
                            .send_to_section(
                                Message::NodeCmd {
                                    cmd: Transfers(ValidateSectionPayout(event.signed_transfer)),
                                    id: MessageId::new(),
                                },
                                true,
                            )
                            .await
                    }
                }
            }
            Ok(None) => Outcome::oki_no_change(), // Would indicate that this apparently has already been done, so no change.
            Err(e) => {
                error!("Error at creating transfer during transition");
                Outcome::error(Error::NetworkData(e))
            } // This would be a bug! Cannot move on from here, some solution needed.
        }
    }

    /// Will validate and sign the payout, and ask of the replicas to
    /// do the same, and await their responses as to accumulate the result.
    pub async fn initiate_reward_payout(&mut self, payout: Payout) -> Outcome<NodeMessagingDuty> {
        if self.state.finished.contains(&payout.node_id) {
            return Outcome::oki_no_change();
        }
        // if we are transitioning, or having payout in flight, the payout is deferred.
        if self.is_transitioning() || self.has_payout_in_flight() {
            self.state.queued_payouts.push_back(payout);
            return Outcome::oki_no_change();
        }

        use NodeCmd::*;
        use NodeTransferCmd::*;
        // We try initiate the transfer..
        match self.actor.transfer(payout.amount, payout.to) {
            Ok(Some(event)) => {
                match self.apply(TransferInitiated(event.clone())) {
                    Err(e) => {
                        // This would be a bug!
                        error!("Error at applying transfer initiation at reward payout!");
                        Outcome::error(Error::NetworkData(e))
                    }
                    Ok(_) => {
                        // We now have a payout in flight.
                        self.state.payout_in_flight = Some(payout);
                        // We ask of our Replicas to validate this transfer.
                        self.wrapping
                            .send_to_section(
                                Message::NodeCmd {
                                    cmd: Transfers(ValidateSectionPayout(event.signed_transfer)),
                                    id: MessageId::new(),
                                },
                                true,
                            )
                            .await
                    }
                }
            }
            Ok(None) => Outcome::oki_no_change(), // Would indicate that this apparently has already been done, so no change.
            Err(e) => Outcome::error(Error::NetworkData(e)), // for now, but should give NodeCmdError
        }
    }

    /// As all Replicas have accumulated the distributed
    /// actor cmds and applied them, they'll send out the
    /// result, which each actor instance accumulates locally.
    pub async fn receive(&mut self, validation: TransferValidated) -> Outcome<NodeOperation> {
        use NodeCmd::*;
        use NodeTransferCmd::*;
        match self.actor.receive(validation) {
            Ok(Some(event)) => {
                match self.apply(TransferValidationReceived(event.clone())) {
                    Err(e) => {
                        // This would be a bug!
                        error!("Error at creating transfer during transition");
                        Outcome::error(Error::NetworkData(e))
                    }
                    Ok(_) => {
                        let proof = if let Some(proof) = event.proof {
                            proof
                        } else {
                            return Ok(None);
                        };
                        // If we have an accumulated proof, we'll update local state.
                        match self.actor.register(proof.clone()) {
                            Ok(None) => (),
                            Ok(Some(event)) => self.apply(TransferRegistrationSent(event))?,
                            Err(e) => return Outcome::error(Error::NetworkData(e)), // for now, but should give NodeCmdError
                        };

                        // The payout flow is completed,
                        // thus we have no payout in flight;
                        if let Some(payout) = self.state.payout_in_flight.take() {
                            let _ = self.state.finished.insert(payout.node_id);
                        }

                        // If we are transitioning to a new actor,
                        // we replace the old with the new.
                        self.try_transition(proof.clone())?;

                        // If there are queued payouts,
                        // the first in queue will be executed.
                        let queued_op = self.try_pop_queue().await;

                        // We ask of our Replicas to register this transfer.
                        let reg_op = match self
                            .wrapping
                            .send_to_section(
                                Message::NodeCmd {
                                    cmd: Transfers(RegisterSectionPayout(proof)),
                                    id: MessageId::new(),
                                },
                                true,
                            )
                            .await?
                        {
                            Some(op) => op.into(),
                            None => return Outcome::error(Error::Logic),
                        };

                        if let Some(queued) = queued_op {
                            // First register the transfer, then
                            // carry out the first queued payout.
                            return Outcome::oki(vec![reg_op, queued].into());
                        }

                        Outcome::oki(reg_op)
                    }
                }
            }
            Ok(None) => Outcome::oki_no_change(),
            Err(e) => Outcome::error(Error::NetworkData(e)), // for now, but should give NodeCmdError
        }
    }

    // Can safely be called without overwriting any
    // payout in flight, since validations for that are made.
    async fn try_pop_queue(&mut self) -> Option<NodeOperation> {
        if let Some(payout) = self.state.queued_payouts.pop_front() {
            // Validation logic when inititating rewards prevents enqueueing a payout that is already
            // in the finished set. Therefore, calling initiate here cannot return None because of
            // the payout already being finished.
            // For that reason it is safe to enqueue it again, if this call returns None.
            // (we will not loop on that payout)
            if let Ok(Some(msg)) = self.initiate_reward_payout(payout.clone()).await {
                return Some(msg.into());
            } else if !self.state.finished.contains(&payout.node_id) {
                // buut.. just to prevent any future changes to
                // enable such a loop, we do the check above anyway :)
                // (NB: We put it at the front of the queue again,
                //  since that's where the other instances will expect it to be. (Unclear atm if this is necessary or not.))
                self.state.queued_payouts.insert(0, payout);
            }
        }
        None
    }

    // If we are transitioning to a new actor, we replace the old with the new.
    fn try_transition(&mut self, credit: DebitAgreementProof) -> Result<()> {
        if !self.is_transition_credit(&credit) {
            return Ok(());
        }
        // hmm.. it would actually be a bug
        // if we have a payout in flight...
        if self.has_payout_in_flight() {
            panic!("You failed to implement the logic correctly. Go back to the drawing desk.")
        }

        use sn_data_types::ReplicaEvent::*;
        // Set the next actor to be our current.
        self.actor = self.state.next_actor.take().unwrap();
        // We checked above that next_actor was some,
        // only case this could fail is if we're multi threading here.
        // (which we don't really have reason for here)

        // Credit the transfer to the new actor.
        match self.actor.synch(vec![TransferPropagated(
            sn_data_types::TransferPropagated {
                debit_proof: credit,
                debiting_replicas: self.actor.id(),
                crediting_replica_sig: dummy_sig(),
            },
        )]) {
            Ok(Some(event)) => self.apply(TransfersSynched(event))?,
            Ok(None) => (),
            Err(error) => return Err(error),
        };

        Ok(())
    }

    fn apply(&mut self, event: ActorEvent) -> Result<()> {
        self.actor.apply(event)
    }

    fn is_transition_credit(&self, credit: &DebitAgreementProof) -> bool {
        if let Some(next_actor) = &self.state.next_actor {
            return credit.to() == next_actor.id();
        }
        false
    }

    fn is_transitioning(&self) -> bool {
        self.state.next_actor.is_some()
    }

    fn has_payout_in_flight(&self) -> bool {
        self.state.payout_in_flight.is_some()
    }
}

use bls::SecretKeyShare;
use sn_data_types::SignatureShare;
fn dummy_sig() -> SignatureShare {
    let dummy_shares = SecretKeyShare::default();
    let dummy_sig = dummy_shares.sign("DUMMY MSG");
    SignatureShare {
        index: 0,
        share: dummy_sig,
    }
}
