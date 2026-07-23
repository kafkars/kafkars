//! Indentation-aware parser rejecting unsupported or ambiguous YAML shapes.

use std::collections::BTreeSet;

use super::model::YamlNode;
use super::syntax::{
    has_yaml_indirection, is_block_marker, mapping_pair, normalize_scalar, reject_yaml_indirection,
    sequence_value,
};

pub(crate) fn parse(source: &str) -> Result<YamlNode, String> {
    let lines = significant_lines(source)?;
    if lines.is_empty() {
        return Err("GitHub YAML document is empty".to_owned());
    }
    let mut cursor = 0;
    let root_indent = lines[0].indent;
    let node = parse_node(&lines, &mut cursor, root_indent)?;
    if cursor != lines.len() {
        return Err(format!(
            "line {} begins a second YAML root",
            lines[cursor].number
        ));
    }
    Ok(node)
}

struct Line<'a> {
    number: usize,
    indent: usize,
    content: &'a str,
}

fn significant_lines(source: &str) -> Result<Vec<Line<'_>>, String> {
    let mut lines = Vec::new();
    for (index, raw) in source.lines().enumerate() {
        let leading = raw.len().saturating_sub(raw.trim_start_matches(' ').len());
        if raw[..leading].contains('\t') {
            return Err(format!("line {} uses tab indentation", index + 1));
        }
        let content = raw[leading..].trim_end();
        if content.is_empty() || content.starts_with('#') {
            continue;
        }
        lines.push(Line {
            number: index + 1,
            indent: leading,
            content,
        });
    }
    Ok(lines)
}

fn parse_node(lines: &[Line<'_>], cursor: &mut usize, indent: usize) -> Result<YamlNode, String> {
    let Some(line) = lines.get(*cursor) else {
        return Err("YAML node has no content".to_owned());
    };
    if line.indent != indent {
        return Err(format!(
            "line {} has indentation {}, expected {indent}",
            line.number, line.indent
        ));
    }
    if sequence_value(line.content).is_some() {
        parse_sequence(lines, cursor, indent)
    } else if mapping_pair(line.content).is_some() {
        parse_mapping(lines, cursor, indent)
    } else {
        reject_yaml_indirection(line.content, line.number)?;
        *cursor += 1;
        Ok(YamlNode::Scalar(normalize_scalar(
            line.content,
            line.number,
        )?))
    }
}

fn parse_mapping(
    lines: &[Line<'_>],
    cursor: &mut usize,
    indent: usize,
) -> Result<YamlNode, String> {
    let mut entries = Vec::new();
    let mut keys = BTreeSet::new();
    while let Some(line) = lines.get(*cursor) {
        if line.indent < indent {
            break;
        }
        if line.indent > indent {
            return Err(format!("line {} has an unexpected child", line.number));
        }
        if sequence_value(line.content).is_some() {
            break;
        }
        let Some((key, value)) = mapping_pair(line.content) else {
            return Err(format!("line {} is not a mapping entry", line.number));
        };
        *cursor += 1;
        insert_mapping_value(
            lines,
            cursor,
            indent,
            key,
            value,
            &mut keys,
            &mut entries,
            line.number,
        )?;
    }
    Ok(YamlNode::Mapping(entries))
}

fn parse_sequence(
    lines: &[Line<'_>],
    cursor: &mut usize,
    indent: usize,
) -> Result<YamlNode, String> {
    let mut items = Vec::new();
    while let Some(line) = lines.get(*cursor) {
        if line.indent < indent {
            break;
        }
        if line.indent > indent {
            return Err(format!(
                "line {} has an unexpected sequence child",
                line.number
            ));
        }
        let Some(value) = sequence_value(line.content) else {
            break;
        };
        let number = line.number;
        *cursor += 1;
        items.push(parse_sequence_item(lines, cursor, indent, value, number)?);
    }
    Ok(YamlNode::Sequence(items))
}

fn parse_sequence_item(
    lines: &[Line<'_>],
    cursor: &mut usize,
    sequence_indent: usize,
    value: &str,
    number: usize,
) -> Result<YamlNode, String> {
    if value.is_empty() {
        return parse_child_or_null(lines, cursor, sequence_indent);
    }
    let Some((key, value)) = mapping_pair(value) else {
        reject_yaml_indirection(value, number)?;
        if lines
            .get(*cursor)
            .is_some_and(|line| line.indent > sequence_indent)
        {
            return Err(format!(
                "line {number} gives a scalar sequence item children"
            ));
        }
        return Ok(YamlNode::Scalar(normalize_scalar(value, number)?));
    };

    let item_indent = sequence_indent + 2;
    let mut entries = Vec::new();
    let mut keys = BTreeSet::new();
    insert_mapping_value(
        lines,
        cursor,
        item_indent,
        key,
        value,
        &mut keys,
        &mut entries,
        number,
    )?;
    while let Some(line) = lines.get(*cursor) {
        if line.indent <= sequence_indent {
            break;
        }
        if line.indent != item_indent {
            return Err(format!("line {} has an unexpected step child", line.number));
        }
        let Some((key, value)) = mapping_pair(line.content) else {
            return Err(format!("line {} is not a step mapping entry", line.number));
        };
        let number = line.number;
        *cursor += 1;
        insert_mapping_value(
            lines,
            cursor,
            item_indent,
            key,
            value,
            &mut keys,
            &mut entries,
            number,
        )?;
    }
    Ok(YamlNode::Mapping(entries))
}

#[allow(clippy::too_many_arguments)]
fn insert_mapping_value(
    lines: &[Line<'_>],
    cursor: &mut usize,
    indent: usize,
    key: &str,
    value: &str,
    keys: &mut BTreeSet<String>,
    entries: &mut Vec<(String, YamlNode)>,
    number: usize,
) -> Result<(), String> {
    if key.is_empty() || key.chars().any(char::is_whitespace) {
        return Err(format!("line {number} has an unsupported mapping key"));
    }
    let key = normalize_scalar(key, number)?;
    if key == "<<" || has_yaml_indirection(&key) || has_yaml_indirection(value) {
        return Err(format!(
            "line {number} uses unsupported YAML merge, anchor, alias, or tag syntax"
        ));
    }
    if !keys.insert(key.clone()) {
        return Err(format!("line {number} duplicates YAML key `{key}`"));
    }
    let node = if is_block_marker(value) {
        parse_block(lines, cursor, indent, value)
    } else if value.is_empty() {
        parse_child_or_null(lines, cursor, indent)?
    } else {
        YamlNode::Scalar(normalize_scalar(value, number)?)
    };
    entries.push((key, node));
    Ok(())
}

fn parse_child_or_null(
    lines: &[Line<'_>],
    cursor: &mut usize,
    parent_indent: usize,
) -> Result<YamlNode, String> {
    match lines.get(*cursor) {
        Some(line) if line.indent > parent_indent => parse_node(lines, cursor, line.indent),
        Some(_) | None => Ok(YamlNode::Null),
    }
}

fn parse_block(
    lines: &[Line<'_>],
    cursor: &mut usize,
    parent_indent: usize,
    style: &str,
) -> YamlNode {
    let mut block = Vec::new();
    while let Some(line) = lines.get(*cursor) {
        if line.indent <= parent_indent {
            break;
        }
        block.push(line.content.trim().to_owned());
        *cursor += 1;
    }
    YamlNode::Block {
        style: style.to_owned(),
        lines: block,
    }
}
