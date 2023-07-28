use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use ella_common::OffsetDateTime;
use ella_engine::{engine::EllaState, EllaConfig};
use hmac::{Hmac, Mac};
use jwt::{RegisteredClaims, SignWithKey, VerifyWithKey};
use sha2::Sha256;
use tonic::service::Interceptor;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct ConnectionState {
    state: Arc<Mutex<EllaState>>,
}

impl ConnectionState {
    pub fn new(state: EllaState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn read(&self) -> EllaState {
        self.state.lock().unwrap().clone()
    }

    pub fn set_config(&self, config: EllaConfig) {
        self.state.lock().unwrap().with_config(config);
    }
}

#[derive(Debug)]
pub(crate) struct AuthProvider {
    key: Hmac<Sha256>,
}

impl AuthProvider {
    pub fn from_secret(secret: &[u8]) -> crate::Result<Self> {
        let key = Hmac::new_from_slice(secret).map_err(|_| crate::ServerError::InvalidSecret)?;
        Ok(Self { key })
    }

    fn encode<T: serde::Serialize>(&self, token: &T) -> crate::Result<String> {
        token
            .sign_with_key(&self.key)
            .map_err(|err| crate::ServerError::Token(err.to_string()).into())
    }

    fn extract_payload<T: serde::de::DeserializeOwned>(
        &self,
        request: &tonic::Request<()>,
    ) -> Result<Option<T>, tonic::Status> {
        match request.metadata().get("authorization").map(|m| m.to_str()) {
            Some(Ok(auth)) => match auth.split_once(' ') {
                Some(("Bearer", token)) => {
                    let payload = token.verify_with_key(&self.key).map_err(|err| {
                        tonic::Status::unauthenticated(format!("invalid token: {}", err))
                    })?;
                    Ok(Some(payload))
                }
                _ => Ok(None),
            },
            Some(Err(_)) => Err(tonic::Status::unauthenticated(
                "unable to parse authorization header as ASCII",
            )),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct ConnectionToken(RegisteredClaims);

impl ConnectionToken {
    fn new(user: Option<String>) -> Self {
        let id = Uuid::new_v4().simple().to_string();
        let issued = OffsetDateTime::now_utc().unix_timestamp().unsigned_abs();
        Self(RegisteredClaims {
            issuer: None,
            subject: user,
            audience: None,
            expiration: None,
            not_before: None,
            issued_at: Some(issued),
            json_web_token_id: Some(id),
        })
    }

    fn uuid(&self) -> Result<Uuid, uuid::Error> {
        Uuid::try_parse(self.0.json_web_token_id.as_deref().unwrap_or_default())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionManager {
    state: EllaState,
    auth: Arc<AuthProvider>,
    connections: Arc<DashMap<Uuid, ConnectionState>>,
}

impl ConnectionManager {
    pub fn new(auth: Arc<AuthProvider>, state: EllaState) -> Self {
        Self {
            auth,
            state,
            connections: Arc::new(DashMap::new()),
        }
    }

    pub fn handshake(&self) -> crate::Result<String> {
        let conn = ConnectionToken::new(None);
        let token = self.auth.encode(&conn)?;
        let state = ConnectionState::new(self.state.clone());
        self.connections.insert(
            conn.uuid()
                .expect("newly created UUID should always be valid"),
            state,
        );
        Ok(token)
    }
}

impl Interceptor for ConnectionManager {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(token) = self.auth.extract_payload::<ConnectionToken>(&request)? {
            let key = token.uuid().map_err(|err| {
                tonic::Status::unauthenticated(format!("invalid connection id: {}", err))
            })?;
            let conn = match self.connections.get(&key) {
                Some(conn) => conn.value().clone(),
                None => {
                    return Err(tonic::Status::unauthenticated(
                        "no active connection found for connection id",
                    ))
                }
            };
            request.extensions_mut().insert(conn);
        }

        Ok(request)
    }
}

pub(crate) fn connection<T>(request: &tonic::Request<T>) -> Result<ConnectionState, tonic::Status> {
    request
        .extensions()
        .get()
        .cloned()
        .ok_or_else(|| tonic::Status::unauthenticated("missing connection token"))
}
