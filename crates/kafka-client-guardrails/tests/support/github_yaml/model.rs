//! Minimal lossless-enough node model for guarded GitHub YAML structure.

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum YamlNode {
    Mapping(Vec<(String, YamlNode)>),
    Sequence(Vec<YamlNode>),
    Scalar(String),
    Block { style: String, lines: Vec<String> },
    Null,
}

impl YamlNode {
    pub(crate) fn mapping(&self) -> Option<&[(String, Self)]> {
        match self {
            Self::Mapping(entries) => Some(entries),
            Self::Sequence(_) | Self::Scalar(_) | Self::Block { .. } | Self::Null => None,
        }
    }

    pub(crate) fn sequence(&self) -> Option<&[Self]> {
        match self {
            Self::Sequence(items) => Some(items),
            Self::Mapping(_) | Self::Scalar(_) | Self::Block { .. } | Self::Null => None,
        }
    }

    pub(crate) fn scalar(&self) -> Option<&str> {
        match self {
            Self::Scalar(value) => Some(value),
            Self::Mapping(_) | Self::Sequence(_) | Self::Block { .. } | Self::Null => None,
        }
    }

    pub(crate) fn block(&self) -> Option<&[String]> {
        match self {
            Self::Block { style, lines } if style == "|" => Some(lines),
            Self::Block { .. }
            | Self::Mapping(_)
            | Self::Sequence(_)
            | Self::Scalar(_)
            | Self::Null => None,
        }
    }
}

pub(crate) fn entry<'a>(mapping: &'a [(String, YamlNode)], key: &str) -> Option<&'a YamlNode> {
    mapping
        .iter()
        .find_map(|(candidate, value)| (candidate == key).then_some(value))
}
