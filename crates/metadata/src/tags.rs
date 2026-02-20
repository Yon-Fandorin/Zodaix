use serde::{Deserialize, Serialize};

/// Tag metadata associated with a file via xattr.
/// Stored as JSON array in `user.zodaix.tags`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Tags(pub Vec<String>);

impl Tags {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        let tags: Vec<String> = serde_json::from_slice(data)?;
        Ok(Self(tags))
    }

    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.0)
    }

    pub fn add(&mut self, tag: &str) {
        let tag = tag.to_string();
        if !self.0.contains(&tag) {
            self.0.push(tag);
        }
    }

    pub fn remove(&mut self, tag: &str) -> bool {
        if let Some(pos) = self.0.iter().position(|t| t == tag) {
            self.0.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, tag: &str) -> bool {
        self.0.iter().any(|t| t == tag)
    }

    pub fn list(&self) -> &[String] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// xattr key conventions for Zodaix metadata.
pub mod xattr_keys {
    /// JSON array of tags: `["tag1", "tag2"]`
    pub const TAGS: &str = "user.zodaix.tags";
    /// UTF-8 description string.
    pub const DESCRIPTION: &str = "user.zodaix.description";
    /// AI-generated summary.
    pub const AI_SUMMARY: &str = "user.zodaix.ai.summary";
    /// AI embedding identifier.
    pub const AI_EMBEDDING_ID: &str = "user.zodaix.ai.embedding_id";
    /// MIME type override.
    pub const MIME_TYPE: &str = "user.zodaix.mime_type";
    /// Custom user attributes prefix.
    pub const CUSTOM_PREFIX: &str = "user.zodaix.custom.";

    /// Check if a key belongs to zodaix namespace.
    pub fn is_zodaix_key(key: &str) -> bool {
        key.starts_with("user.zodaix.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tags_roundtrip() {
        let mut tags = Tags::new();
        tags.add("rust");
        tags.add("vfs");
        tags.add("rust"); // duplicate

        assert_eq!(tags.list().len(), 2);
        assert!(tags.contains("rust"));

        let json = tags.to_json().unwrap();
        let restored = Tags::from_json(&json).unwrap();
        assert_eq!(restored.list(), tags.list());
    }

    #[test]
    fn test_tags_remove() {
        let mut tags = Tags::new();
        tags.add("a");
        tags.add("b");

        assert!(tags.remove("a"));
        assert!(!tags.remove("c"));
        assert!(!tags.contains("a"));
        assert!(tags.contains("b"));
    }
}
