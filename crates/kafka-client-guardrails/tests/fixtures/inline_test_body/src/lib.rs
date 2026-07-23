//! Invalid inline test and placeholder fixture.

pub fn unfinished() {
    todo!()
}

#[cfg(test)]
mod tests {
    #[test]
    fn hidden_in_production_source() {}
}
