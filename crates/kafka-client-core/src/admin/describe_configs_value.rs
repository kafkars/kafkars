//! Wire-free scalar configuration values retained by `DescribeConfigs`.

/// One deterministic configuration synonym.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigSynonym {
    name: String,
    value: Option<String>,
    source: i8,
}

impl DescribeConfigSynonym {
    /// Creates one protocol-normalized synonym.
    pub const fn new(name: String, value: Option<String>, source: i8) -> Self {
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

    /// Consumes this synonym into adapter-owned parts.
    pub fn into_parts(self) -> (String, Option<String>, i8) {
        (self.name, self.value, self.source)
    }
}

/// One protocol-normalized configuration entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigEntry {
    name: String,
    value: Option<String>,
    read_only: bool,
    source: i8,
    sensitive: bool,
    synonyms: Vec<DescribeConfigSynonym>,
    config_type: Option<i8>,
    documentation: Option<String>,
}

impl DescribeConfigEntry {
    /// Creates one bounded wire-free configuration fact.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        name: String,
        value: Option<String>,
        read_only: bool,
        source: i8,
        sensitive: bool,
        synonyms: Vec<DescribeConfigSynonym>,
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

    /// Returns the nullable configuration value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns whether Kafka marks this configuration read-only.
    pub const fn read_only(&self) -> bool {
        self.read_only
    }

    /// Returns Kafka's exact signed configuration source.
    pub const fn source(&self) -> i8 {
        self.source
    }

    /// Returns whether Kafka marks this value sensitive.
    pub const fn sensitive(&self) -> bool {
        self.sensitive
    }

    /// Returns deterministic synonyms requested from Kafka.
    pub fn synonyms(&self) -> &[DescribeConfigSynonym] {
        &self.synonyms
    }

    /// Returns the version-present configuration type.
    pub const fn config_type(&self) -> Option<i8> {
        self.config_type
    }

    /// Returns version-present nullable documentation.
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }

    /// Consumes this entry into adapter-owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<String>,
        bool,
        i8,
        bool,
        Vec<DescribeConfigSynonym>,
        Option<i8>,
        Option<String>,
    ) {
        (
            self.name,
            self.value,
            self.read_only,
            self.source,
            self.sensitive,
            self.synonyms,
            self.config_type,
            self.documentation,
        )
    }
}
