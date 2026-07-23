//! Forbidden capability hidden behind an import alias.

use std::net as networking;

pub fn socket_type() -> Option<networking::TcpStream> {
    None
}
