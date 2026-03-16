use super::Ros1TopicConfig;
use anyhow::{Error, anyhow};
use log::{info, warn};
use std::collections::HashSet;

pub(super) fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    let mut pos = 0usize;
    let mut first = true;

    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if first && !pattern.starts_with('*') {
            if !value[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
            first = false;
            continue;
        }

        let is_last = idx == parts.len() - 1;
        if is_last && !pattern.ends_with('*') {
            return value[pos..].ends_with(part);
        }

        match value[pos..].find(part) {
            Some(found) => {
                pos += found + part.len();
            }
            None => return false,
        }
        first = false;
    }

    true
}

pub(super) fn resolve_topic_patterns(
    configured_topics: &[Ros1TopicConfig],
    available_topics: &[String],
) -> Vec<Ros1TopicConfig> {
    let mut resolved = Vec::new();
    let mut subscribed_topics = HashSet::new();

    for topic_cfg in configured_topics {
        let pattern = topic_cfg.name.as_str();
        if pattern.contains('*') {
            for topic_name in available_topics {
                if wildcard_match(pattern, topic_name)
                    && subscribed_topics.insert(topic_name.clone())
                {
                    let mut expanded = topic_cfg.clone();
                    expanded.name = topic_name.clone();
                    resolved.push(expanded);
                }
            }
        } else if subscribed_topics.insert(pattern.to_string()) {
            resolved.push(topic_cfg.clone());
        }
    }

    resolved
}

pub(super) fn resolve_topics_for_subscription(
    configured_topics: &[Ros1TopicConfig],
) -> Result<Vec<Ros1TopicConfig>, Error> {
    let available_topics = rosrust::topics()
        .map_err(|err| {
            anyhow!(
                "Failed to fetch ROS topics for wildcard resolution: {}",
                err
            )
        })?
        .into_iter()
        .map(|topic| topic.name)
        .collect::<Vec<_>>();

    for topic_cfg in configured_topics {
        if topic_cfg.name.contains('*') {
            let matches = available_topics
                .iter()
                .filter(|topic| wildcard_match(&topic_cfg.name, topic))
                .count();
            if matches == 0 {
                warn!(
                    "No ROS topics matched wildcard pattern '{}' during startup discovery",
                    topic_cfg.name
                );
            } else {
                info!(
                    "Resolved ROS wildcard '{}' to {} topic(s)",
                    topic_cfg.name, matches
                );
            }
        }
    }

    Ok(resolve_topic_patterns(configured_topics, &available_topics))
}

#[cfg(test)]
mod tests {
    use super::{resolve_topic_patterns, wildcard_match};
    use crate::input::ros1::Ros1TopicConfig;

    fn topic(name: &str, entry_name: &str) -> Ros1TopicConfig {
        Ros1TopicConfig {
            name: name.to_string(),
            entry_name: Some(entry_name.to_string()),
            labels: Vec::new(),
        }
    }

    #[test]
    fn wildcard_match_handles_common_cases() {
        assert!(wildcard_match("*", "/camera/front/image"));
        assert!(wildcard_match("/camera/*", "/camera/front/image"));
        assert!(wildcard_match("*/image", "/camera/front/image"));
        assert!(wildcard_match("/camera/*/image", "/camera/front/image"));
        assert!(!wildcard_match("/lidar/*", "/camera/front"));
        assert!(!wildcard_match("/camera", "/camera/front"));
    }

    #[test]
    fn resolve_topic_patterns_expands_wildcards_and_deduplicates() {
        let configured = vec![
            topic("/camera/*", "camera"),
            topic("/lidar/points", "lidar"),
        ];
        let available = vec![
            "/camera/front".to_string(),
            "/camera/rear".to_string(),
            "/lidar/points".to_string(),
        ];

        let resolved = resolve_topic_patterns(&configured, &available);
        let names: Vec<String> = resolved.into_iter().map(|cfg| cfg.name).collect();

        assert_eq!(
            names,
            vec!["/camera/front", "/camera/rear", "/lidar/points"]
        );
    }

    #[test]
    fn resolve_topic_patterns_keeps_first_config_for_overlapping_topics() {
        let configured = vec![
            topic("/camera/front", "front"),
            topic("/camera/*", "camera"),
        ];
        let available = vec!["/camera/front".to_string(), "/camera/rear".to_string()];

        let resolved = resolve_topic_patterns(&configured, &available);

        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].name, "/camera/front");
        assert_eq!(resolved[0].entry_name, Some("front".to_string()));
        assert_eq!(resolved[1].name, "/camera/rear");
        assert_eq!(resolved[1].entry_name, Some("camera".to_string()));
    }
}
