pub enum SendError<T> {
    Disconnected(T),
}