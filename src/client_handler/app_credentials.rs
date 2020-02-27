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

type AppCredentialsAsTuple = (BTreeMap<PublicKey, [u8; 32]>, u64);

const AUTH_KEYS_DB_NAME: &str = "auth_keys.db";

#[derive(Default, Serialize, Deserialize, Debug)]
pub(super) struct AppCredentials {
    apps: HashMap<PublicKey, [u8; 32]>,
    version: u64,
}

impl AppCredentials {
    fn into_tuple(self) -> AppCredentialsAsTuple {
        (self.apps.into_iter().collect(), self.version)
    }
}

pub(super) struct AppCredentialsDb {
    db: PickleDb,
}

impl AppCredentialsDb {
    pub fn new<R: AsRef<Path>>(root_dir: R, init_mode: Init) -> Result<Self> {
        Ok(Self {
            db: utils::new_db(root_dir, AUTH_KEYS_DB_NAME, init_mode)?,
        })
    }

    /// If the specified auth_key doesn't exist, a default `AuthKeysAsTuple` is returned.
    pub fn list_app_credentials_and_version(&self, client_id: &ClientPublicId) -> AppCredentialsAsTuple {
        let db_key = client_id.to_db_key();
        self.db
            .get::<AppCredentials>(&db_key)
            .map(AppCredentials::into_tuple)
            .unwrap_or_default()
    }

    /// Inserts `key` and `permissions` into the specified auth_key, creating the auth_key if it
    /// doesn't already exist.
    pub fn ins_app_credentials(
        &mut self,
        client_id: &ClientPublicId,
        key: PublicKey,
        new_version: u64,
        token: AuthToken,
    ) -> NdResult<()> {
        let db_key = client_id.to_db_key();
        let mut app_creds = self.get_app_credentials_and_increment_version(&db_key, new_version)?;
        let serialized_token = serialize(&token)
            .map_err(|_| NdError::FailedToParse("Error serializing caveats".to_string()))?;
        let hashed_token = sha3_256(serialized_token.as_slice());
        // TODO - should we assert the `key` is an App type?

        let _ = app_creds.apps.insert(key, hashed_token);
        if let Err(error) = self.db.set(&db_key, &app_creds) {
            warn!("Failed to write App Credentials to DB: {:?}", error);
            return Err(NdError::from("Failed to insert App Credentials."));
        }

        Ok(())
    }

    /// Deletes `key` from the specified auth_key.  Returns `NoSuchKey` if the auth_keys or key doesn't
    /// exist.
    pub fn del_app_credentials(
        &mut self,
        client_id: &ClientPublicId,
        key: PublicKey,
        new_version: u64,
    ) -> NdResult<()> {
        let db_key = client_id.to_db_key();
        let mut app_creds = self.get_app_credentials_and_increment_version(&db_key, new_version)?;
        if app_creds.apps.remove(&key).is_none() {
            trace!("Failed to delete non-existent authorised key {}", key);
            return Err(NdError::NoSuchKey);
        }
        if let Err(error) = self.db.set(&db_key, &app_creds) {
            warn!("Failed to write AppCredentials to DB: {:?}", error);
            return Err(NdError::from("Failed to insert App Credentials."));
        }

        Ok(())
    }

    fn get_app_credentials_and_increment_version(
        &self,
        db_key: &str,
        new_version: u64,
    ) -> NdResult<AppCredentials> {
        let mut app_creds = self.db.get::<AppCredentials>(&db_key).unwrap_or_default();

        if app_creds.version + 1 != new_version {
            trace!(
                "Failed to mutate authorised key.  Current version: {}  New version: {}",
                app_creds.version,
                new_version
            );
            return Err(NdError::InvalidSuccessor(app_creds.version));
        }
        app_creds.version = new_version;
        Ok(app_creds)
    }
}
