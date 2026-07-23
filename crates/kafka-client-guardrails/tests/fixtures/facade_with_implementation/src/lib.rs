//! Invalid facade fixture.

mod declared;

use declared::Declared;
pub(self) use declared::Declared as SelfDeclared;

pub fn implementation_smuggled_into_facade() {}

mod hidden {
    pub fn implementation_smuggled_into_inline_module() {}
}
