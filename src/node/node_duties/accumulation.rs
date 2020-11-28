// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{utils, Error, Outcome, TernaryResult};
use log::{error, info};
use sn_data_types::{
    Duty, MessageId, MsgEnvelope, MsgSender, SignatureShare, TransientSectionKey as SectionKey,
};
use std::collections::{hash_map::Entry, HashMap, HashSet};

type RequestInfo = (MsgEnvelope, MsgSender, Vec<SignatureShare>);
//type DuplicationInfo = (BlobAddress, BTreeSet<XorName>, Vec<SignatureShare>);

/// Accumulation of messages signed by individual nodes
/// into messages that are stamped as originating from a section.
/// This happens when enough signatures have been accumulated.
pub struct Accumulation {
    messages: HashMap<MessageId, RequestInfo>,
    completed: HashSet<MessageId>,
}

impl Accumulation {
    pub fn new() -> Self {
        Self {
            messages: Default::default(),
            completed: Default::default(),
        }
    }

    pub(crate) fn process_message_envelope(&mut self, msg: &MsgEnvelope) -> Outcome<MsgEnvelope> {
        if self.completed.contains(&msg.id()) {
            info!("Message already processed.");
            return Outcome::oki_no_change();
        }
        if msg.most_recent_sender().is_section() {
            info!("Received message sent by a Section. No need to accumulate");
            return Outcome::oki(msg.clone()); // already group signed, no need to accumulate (check sig though?, or somewhere else, earlier on?)
        }

        let sender = msg.most_recent_sender();
        if !sender.is_elder() {
            error!("Only Elder messages are accumulated!");
            return Outcome::error(Error::Logic);
        }
        let sig_share = match sender.group_sig_share() {
            Some(sig_share) => sig_share,
            None => {
                error!("No sig share found!");
                return Outcome::error(Error::Logic);
            }
        };

        info!(
            "{}: Accumulating signatures for {:?}",
            "should be id here",
            //&id,
            msg.id()
        );
        match self.messages.entry(msg.id()) {
            Entry::Vacant(entry) => {
                let _ = entry.insert((msg.clone(), msg.origin.clone(), vec![sig_share]));
            }
            Entry::Occupied(mut entry) => {
                let (_, _, signatures) = entry.get_mut();
                signatures.push(sig_share);
            }
        }
        self.try_aggregate(msg)
    }

    fn try_aggregate(&mut self, msg: &MsgEnvelope) -> Outcome<MsgEnvelope> {
        let msg_id = msg.id();
        let signatures = match self.messages.get(&msg_id) {
            Some((_, _, signatures)) => signatures,
            None => {
                error!("No such message id! ({:?})", msg_id);
                return Outcome::error(Error::Logic);
            }
        };

        let sender = msg.most_recent_sender();
        if !sender.is_elder() {
            error!("Only Elder messages are accumulated!");
            return Outcome::error(Error::Logic);
        }
        let public_key_set = match sender.group_key_set() {
            Some(public_key_set) => public_key_set,
            None => {
                error!("No public_key_set found!");
                return Outcome::error(Error::Logic);
            }
        };

        info!(
            "Got {} signatures. We need {}",
            signatures.len(),
            public_key_set.threshold() + 1
        );
        if public_key_set.threshold() >= signatures.len() {
            return Outcome::oki_no_change();
        }
        let (msg, _sender, signatures) = match self.messages.remove(&msg_id) {
            Some((msg, _sender, signatures)) => (msg, _sender, signatures),
            None => {
                error!("No such message id! ({:?})", msg_id);
                return Outcome::error(Error::Logic);
            }
        };
        let signed_data = utils::serialise(&msg);
        for sig in &signatures {
            if !public_key_set
                .public_key_share(sig.index)
                .verify(&sig.share, &signed_data)
            {
                error!("Invalid signature share");
                // should not just return, instead:
                // remove the faulty sig, then insert
                // the rest back into messages.
                // One bad egg can't be allowed to ruin it all.
                return Outcome::error(Error::Logic);
            }
        }
        let signature = public_key_set
            .combine_signatures(signatures.iter().map(|sig| (sig.index, &sig.share)))
            .map_err(|e| {
                error!("{}", e);
                Error::Logic
            })?;
        if public_key_set.public_key().verify(&signature, &signed_data) {
            let _ = self.completed.insert(msg_id);

            // upgrade sender to Section, since it accumulated
            let section_key = SectionKey {
                bls_key: public_key_set.public_key(),
            };
            if let Some(Duty::Elder(duty)) = sender.duty() {
                let sender = MsgSender::section(section_key, duty)?;
                // Replace the Node with the Section.
                let mut msg = msg;
                let _ = msg.proxies.pop();
                msg.add_proxy(sender);
                Outcome::oki(msg)
            } else {
                error!("Only Elder messages are accumulated!");
                unreachable!() // this condition is tested further up in this method..
                               //Outcome::error(Error::Logic)
            }
        // beware that we might have to forgo the proxies vector
        // and instead just have a most recent proxy, if we are seeing
        // different order on the proxies on the msgs to be accumulated
        // (otherwise, the signature won't aggregate, since it is not over the same data)
        // perhaps it can be solved by ordering the vec, but maybe that defeats
        // part of the purpose; to see the path.
        } else {
            error!("Accumulated signature is invalid");
            Outcome::error(Error::Logic)
        }
    }
}
