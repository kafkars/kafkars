//! Stable wire-free configuration facts returned by `DescribeConfigs`.

/// One Kafka configuration synonym.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    name: String,
    value: Option<String>,
    source: i8,
}

impl ConfigSynonym {
    pub(crate) const fn new(name: String, value: Option<String>, source: i8) -> Self {
        Self {
            name,
            value,
            source,
        }
    }

    /// Returns the synonym name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nullable synonym value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns Kafka's exact signed configuration source.
    pub const fn source(&self) -> i8 {
        self.source
    }
}

/// One Kafka topic configuration entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    name: String,
    value: Option<String>,
    read_only: bool,
    source: i8,
    sensitive: bool,
    synonyms: Vec<ConfigSynonym>,
    config_type: Option<i8>,
    documentation: Option<String>,
}

impl ConfigEntry {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        name: String,
        value: Option<String>,
        read_only: bool,
        source: i8,
        sensitive: bool,
        synonyms: Vec<ConfigSynonym>,
        config_type: Option<i8>,
        documentation: Option<String>,
    ) -> Self {
        Self {
            name,
            value,
            read_only,
            source,
            sensitive,
            synonyms,
            config_type,
            documentation,
        }
    }

    /// Returns the configuration name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nullable value. Sensitive configurations commonly omit it.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns whether Kafka marks the configuration read-only.
    pub const fn read_only(&self) -> bool {
        self.read_only
    }

    /// Returns Kafka's exact signed configuration source.
    pub const fn source(&self) -> i8 {
        self.source
    }

    /// Returns whether Kafka marks the configuration sensitive.
    pub const fn sensitive(&self) -> bool {
        self.sensitive
    }

    /// Returns synonyms in Kafka's normalized deterministic order.
    pub fn synonyms(&self) -> &[ConfigSynonym] {
        &self.synonyms
    }

    /// Returns the configuration type when supplied by the negotiated version.
    pub const fn config_type(&self) -> Option<i8> {
        self.config_type
    }

    /// Returns nullable documentation when supplied by the negotiated version.
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }
}
