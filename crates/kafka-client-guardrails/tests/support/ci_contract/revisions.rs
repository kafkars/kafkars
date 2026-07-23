//! Reviewed sibling revisions are inert canonical data, never shell input.

pub(crate) fn violations(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let lines = source.lines().collect::<Vec<_>>();
    if lines.len() != 2 {
        violations.push("sibling revision file must contain exactly two lines".to_owned());
        return violations;
    }
    for (line, name) in [
        (lines[0], "KAFKA_DRIVER_REVISION"),
        (lines[1], "KAFKA_PROTOCOL_REVISION"),
    ] {
        let expected_prefix = format!("{name}=");
        let Some(revision) = line.strip_prefix(&expected_prefix) else {
            violations.push(format!(
                "sibling revision line must begin with `{expected_prefix}`"
            ));
            continue;
        };
        if revision.len() != 40
            || !revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            violations.push(format!("{name} must be exactly 40 lowercase hex digits"));
        }
    }
    violations
}
