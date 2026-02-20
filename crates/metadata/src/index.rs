use std::path::PathBuf;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy};
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("Query parse error: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// A search result entry.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub path: String,
    pub ino: u64,
    pub tags: Vec<String>,
    pub description: String,
    pub score: f32,
}

/// Tantivy-based search index for VFS metadata.
pub struct SearchIndex {
    index: Index,
    writer: IndexWriter,
    // Schema fields
    f_path: Field,
    f_ino: Field,
    f_name: Field,
    f_tags: Field,
    f_description: Field,
    f_content_preview: Field,
}

impl SearchIndex {
    /// Create or open a search index at the given directory.
    pub fn open(index_dir: &PathBuf) -> Result<Self, IndexError> {
        std::fs::create_dir_all(index_dir)?;

        let mut schema_builder = Schema::builder();
        let f_path = schema_builder.add_text_field("path", STRING | STORED);
        let f_ino = schema_builder.add_u64_field("ino", INDEXED | STORED);
        let f_name = schema_builder.add_text_field("name", TEXT | STORED);
        let f_tags = schema_builder.add_text_field("tags", TEXT | STORED);
        let f_description = schema_builder.add_text_field("description", TEXT | STORED);
        let f_content_preview = schema_builder.add_text_field("content_preview", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_dir(index_dir, schema.clone())
            .or_else(|_| Index::open_in_dir(index_dir))?;

        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index,
            writer,
            f_path,
            f_ino,
            f_name,
            f_tags,
            f_description,
            f_content_preview,
        })
    }

    /// Create an in-memory search index (for testing).
    pub fn in_memory() -> Result<Self, IndexError> {
        let mut schema_builder = Schema::builder();
        let f_path = schema_builder.add_text_field("path", STRING | STORED);
        let f_ino = schema_builder.add_u64_field("ino", INDEXED | STORED);
        let f_name = schema_builder.add_text_field("name", TEXT | STORED);
        let f_tags = schema_builder.add_text_field("tags", TEXT | STORED);
        let f_description = schema_builder.add_text_field("description", TEXT | STORED);
        let f_content_preview = schema_builder.add_text_field("content_preview", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let writer = index.writer(15_000_000)?;

        Ok(Self {
            index,
            writer,
            f_path,
            f_ino,
            f_name,
            f_tags,
            f_description,
            f_content_preview,
        })
    }

    /// Index or re-index a file's metadata.
    ///
    /// Changes are buffered until `commit()` is called. Call `commit()` after
    /// a batch of upserts for best performance.
    pub fn upsert(
        &mut self,
        path: &str,
        ino: u64,
        name: &str,
        tags: &[String],
        description: &str,
        content_preview: &str,
    ) -> Result<(), IndexError> {
        // Delete existing entry for this path.
        let term = tantivy::Term::from_field_text(self.f_path, path);
        self.writer.delete_term(term);

        let tags_str = tags.join(" ");
        self.writer.add_document(doc!(
            self.f_path => path,
            self.f_ino => ino,
            self.f_name => name,
            self.f_tags => tags_str,
            self.f_description => description,
            self.f_content_preview => content_preview,
        ))?;

        debug!("staged: {path} (ino={ino})");
        Ok(())
    }

    /// Remove a file from the index.
    ///
    /// Changes are buffered until `commit()` is called.
    pub fn remove(&mut self, path: &str) -> Result<(), IndexError> {
        let term = tantivy::Term::from_field_text(self.f_path, path);
        self.writer.delete_term(term);
        Ok(())
    }

    /// Commit all pending changes (upserts and removes) to the index.
    pub fn commit(&mut self) -> Result<(), IndexError> {
        self.writer.commit()?;
        Ok(())
    }

    /// Search the index. Returns up to `limit` results.
    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<SearchResult>, IndexError> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        let searcher = reader.searcher();

        let query_parser = QueryParser::for_index(
            &self.index,
            vec![
                self.f_name,
                self.f_tags,
                self.f_description,
                self.f_content_preview,
            ],
        );
        let query = query_parser.parse_query(query_str)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            let path = doc
                .get_first(self.f_path)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let ino = doc
                .get_first(self.f_ino)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let tags_str = doc
                .get_first(self.f_tags)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tags: Vec<String> = tags_str
                .split_whitespace()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();

            let description = doc
                .get_first(self.f_description)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            results.push(SearchResult {
                path,
                ino,
                tags,
                description,
                score,
            });
        }

        Ok(results)
    }
}

impl Drop for SearchIndex {
    fn drop(&mut self) {
        // Best-effort commit of any pending changes on drop.
        let _ = self.writer.commit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_and_search() {
        let mut idx = SearchIndex::in_memory().unwrap();

        idx.upsert(
            "/test/auth.rs",
            42,
            "auth.rs",
            &["auth".to_string(), "security".to_string()],
            "Authentication logic",
            "fn authenticate(user: &str)",
        )
        .unwrap();

        idx.upsert(
            "/test/main.rs",
            43,
            "main.rs",
            &["entry".to_string()],
            "Application entry point",
            "fn main() { }",
        )
        .unwrap();

        idx.commit().unwrap();

        let results = idx.search("auth", 10).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].path, "/test/auth.rs");
    }

    #[test]
    fn test_search_by_tag() {
        let mut idx = SearchIndex::in_memory().unwrap();

        idx.upsert("/a.rs", 1, "a.rs", &["important".to_string()], "", "")
            .unwrap();
        idx.commit().unwrap();

        let results = idx.search("important", 10).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_remove_from_index() {
        let mut idx = SearchIndex::in_memory().unwrap();

        idx.upsert("/del.rs", 99, "del.rs", &[], "to delete", "")
            .unwrap();
        idx.commit().unwrap();

        idx.remove("/del.rs").unwrap();
        idx.commit().unwrap();

        let results = idx.search("delete", 10).unwrap();
        assert!(results.is_empty());
    }
}
