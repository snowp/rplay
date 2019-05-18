extern crate redis;

use super::api;

pub struct RedisApi {
    pub addr: String,
}

pub struct RedisTlsApi {
    _client: redis::Client,
}

impl api::Api<RedisTlsApi> for RedisApi {
    fn create_tls_api(self: &Self) -> RedisTlsApi {
        RedisTlsApi {
            _client: redis::Client::open(self.addr.as_str()).unwrap(),
        }
    }
}
