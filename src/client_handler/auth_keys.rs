// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{utils, vault::Init, Result, ToDbKey};
use bincode::serialize;
use log::{trace, warn};
use pickledb::PickleDb;
use safe_nd::{AuthToken, ClientPublicId, Error as NdError, PublicKey, Result as NdResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};
use tiny_keccak::sha3_256;

type AuthKeysAsTuple = (BTreeMap<PublicKey, [u8; 32]>, u64);

const AUTH_KEYS_DB_NAME: &str = "auth_keys.db";

#[derive(Default, Serialize, Deserialize, Debug)]
pub(super) struct AuthKeys {
    apps: HashMap<PublicKey, [u8; 32]>,
    version: u64,
}

impl AuthKeys {
    fn into_tuple(self) -> AuthKeysAsTuple {
        (self.apps.into_iter().collect(), self.version)
    }
}

pub(super) struct AuthKeysDb {
    db: PickleDb,
}

impl AuthKeysDb {
    pub fn new<R: AsRef<Path>>(root_dir: R, init_mode: Init) -> Result<Self> {
        Ok(Self {
            db: utils::new_db(root_dir, AUTH_KEYS_DB_NAME, init_mode)?,
        })
    }

    /// If the specified auth_key doesn't exist, a default `AuthKeysAsTuple` is returned.
    pub fn list_auth_keys_and_version(&self, client_id: &ClientPublicId) -> AuthKeysAsTuple {
        let db_key = client_id.to_db_key();
        self.db
            .get::<AuthKeys>(&db_key)
            .map(AuthKeys::into_tuple)
            .unwrap_or_default()
    }

    /// Inserts `key` and `permissions` into the specified auth_key, creating the auth_key if it
    /// doesn't already exist.
    pub fn ins_auth_key(
        &mut self,
        client_id: &ClientPublicId,
        key: PublicKey,
        new_version: u64,
        token: AuthToken,
    ) -> NdResult<()> {
        let db_key = client_id.to_db_key();
        let mut auth_keys = self.get_auth_keys_and_increment_version(&db_key, new_version)?;
        let serialized_token = serialize(&token)
            .map_err(|_| NdError::FailedToParse("Error serializing caveats".to_string()))?;
        let hashed_token = sha3_256(serialized_token.as_slice());
        // TODO - should we assert the `key` is an App type?

        let _ = auth_keys.apps.insert(key, hashed_token);
        if let Err(error) = self.db.set(&db_key, &auth_keys) {
            warn!("Failed to write AuthKey to DB: {:?}", error);
            return Err(NdError::from("Failed to insert authorised key."));
        }

        Ok(())
    }

    /// Deletes `key` from the specified auth_key.  Returns `NoSuchKey` if the auth_keys or key doesn't
    /// exist.
    pub fn del_auth_key(
        &mut self,
        client_id: &ClientPublicId,
        key: PublicKey,
        new_version: u64,
    ) -> NdResult<()> {
        let db_key = client_id.to_db_key();
        let mut auth_keys = self.get_auth_keys_and_increment_version(&db_key, new_version)?;
        if auth_keys.apps.remove(&key).is_none() {
            trace!("Failed to delete non-existent authorised key {}", key);
            return Err(NdError::NoSuchKey);
        }
        if let Err(error) = self.db.set(&db_key, &auth_keys) {
            warn!("Failed to write AuthKey to DB: {:?}", error);
            return Err(NdError::from("Failed to insert authorised key."));
        }

        Ok(())
    }

    fn get_auth_keys_and_increment_version(
        &self,
        db_key: &str,
        new_version: u64,
    ) -> NdResult<AuthKeys> {
        let mut auth_keys = self.db.get::<AuthKeys>(&db_key).unwrap_or_default();

        if auth_keys.version + 1 != new_version {
            trace!(
                "Failed to mutate authorised key.  Current version: {}  New version: {}",
                auth_keys.version,
                new_version
            );
            return Err(NdError::InvalidSuccessor(auth_keys.version));
        }
        auth_keys.version = new_version;
        Ok(auth_keys)
    }
}
