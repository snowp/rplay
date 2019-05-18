pub trait Api<T> {
    fn create_tls_api(self: &Self) -> T;
}