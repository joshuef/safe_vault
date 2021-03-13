// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub mod adult_reader;
mod blob_register;
mod data_stores;
mod map_storage;
mod reading;
mod sequence_storage;
mod writing;

use self::adult_reader::SomethingThatShouldBeQueriedFromRoutingAdultReader;
use super::node_ops::NodeDuty;
use crate::{capacity::ChunkHolderDbs, node::node_ops::NodeDuties, node::Storage, Network, Result};
use blob_register::BlobRegister;
use data_stores::DataStores;
use map_storage::MapStorage;
use sequence_storage::SequenceStorage;
use sn_messaging::{
    client::{DataCmd, DataQuery},
    EndUser, MessageId,
};
use std::fmt::{self, Display, Formatter};
use xor_name::XorName;

/// This module is called `Metadata`
/// as a preparation for the responsibilities
/// it will have eventually, after `Data Hierarchy Refinement`
/// has been implemented; where the data types are all simply
/// the structures + their metadata - handled at `Elders` - with
/// all underlying data being chunks stored at `Adults`.
#[derive(Clone)]
pub struct Metadata {
    data_stores: DataStores,
}

impl Metadata {
    pub async fn new(
        node_info: &Storage,
        dbs: ChunkHolderDbs,
        reader: SomethingThatShouldBeQueriedFromRoutingAdultReader,
    ) -> Result<Self> {
        let blob_register = BlobRegister::new(dbs, reader);
        let map_storage = MapStorage::new(node_info).await?;
        let sequence_storage = SequenceStorage::new(node_info).await?;
        let data_stores = DataStores::new(blob_register, map_storage, sequence_storage);
        Ok(Self { data_stores })
    }

    pub async fn read(&self, query: DataQuery, id: MessageId, origin: EndUser) -> Result<NodeDuty> {
        reading::get_result(query, id, origin, &self.data_stores).await
    }

    pub async fn write(
        &mut self,
        cmd: DataCmd,
        id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        writing::get_result(cmd, id, origin, &mut self.data_stores).await
    }

    // This should be called whenever a node leaves the section. It fetches the list of data that was
    // previously held by the node and requests the other holders to store an additional copy.
    // The list of holders is also updated by removing the node that left.
    pub async fn trigger_chunk_replication(&mut self, node: XorName) -> Result<NodeDuties> {
        self.data_stores
            .blob_register_mut()
            .replicate_chunks(node)
            .await
    }
}

impl Display for Metadata {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Metadata")
    }
}
