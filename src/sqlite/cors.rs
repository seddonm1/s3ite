//! `SQLite` S3 bucket CORS persistence helpers.

use rusqlite::{Error::ToSqlConversionFailure, Transaction};

use super::{Sqlite, SqliteResult, StoredCorsRule};

#[derive(Clone, Copy)]
enum CorsRuleValueTable {
    AllowedOrigin,
    AllowedMethod,
    AllowedHeader,
    ExposeHeader,
}

impl CorsRuleValueTable {
    fn select_sql(self) -> &'static str {
        match self {
            Self::AllowedOrigin => {
                "
                SELECT value
                FROM bucket_cors_allowed_origin
                WHERE rule_sequence = ?1
                ORDER BY position;"
            }
            Self::AllowedMethod => {
                "
                SELECT value
                FROM bucket_cors_allowed_method
                WHERE rule_sequence = ?1
                ORDER BY position;"
            }
            Self::AllowedHeader => {
                "
                SELECT value
                FROM bucket_cors_allowed_header
                WHERE rule_sequence = ?1
                ORDER BY position;"
            }
            Self::ExposeHeader => {
                "
                SELECT value
                FROM bucket_cors_expose_header
                WHERE rule_sequence = ?1
                ORDER BY position;"
            }
        }
    }

    fn insert_sql(self) -> &'static str {
        match self {
            Self::AllowedOrigin => {
                "
                INSERT INTO bucket_cors_allowed_origin (rule_sequence, position, value)
                VALUES (?1, ?2, ?3);"
            }
            Self::AllowedMethod => {
                "
                INSERT INTO bucket_cors_allowed_method (rule_sequence, position, value)
                VALUES (?1, ?2, ?3);"
            }
            Self::AllowedHeader => {
                "
                INSERT INTO bucket_cors_allowed_header (rule_sequence, position, value)
                VALUES (?1, ?2, ?3);"
            }
            Self::ExposeHeader => {
                "
                INSERT INTO bucket_cors_expose_header (rule_sequence, position, value)
                VALUES (?1, ?2, ?3);"
            }
        }
    }
}

impl Sqlite {
    /// Reads bucket CORS rules in persisted rule order.
    pub(crate) fn get_bucket_cors_rules(
        transaction: &Transaction,
    ) -> SqliteResult<Vec<StoredCorsRule>> {
        let mut stmt = transaction.prepare_cached(
            "
            SELECT sequence, id, max_age_seconds
            FROM bucket_cors_rule
            ORDER BY sequence;",
        )?;

        let rows = stmt
            .query_map((), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i32>>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        rows.into_iter()
            .map(|(sequence, id, max_age_seconds)| {
                Ok(StoredCorsRule {
                    id,
                    allowed_origins: Self::get_bucket_cors_rule_values(
                        transaction,
                        CorsRuleValueTable::AllowedOrigin,
                        sequence,
                    )?,
                    allowed_methods: Self::get_bucket_cors_rule_values(
                        transaction,
                        CorsRuleValueTable::AllowedMethod,
                        sequence,
                    )?,
                    allowed_headers: Self::get_bucket_cors_rule_values(
                        transaction,
                        CorsRuleValueTable::AllowedHeader,
                        sequence,
                    )?,
                    expose_headers: Self::get_bucket_cors_rule_values(
                        transaction,
                        CorsRuleValueTable::ExposeHeader,
                        sequence,
                    )?,
                    max_age_seconds,
                })
            })
            .collect()
    }

    /// Replaces all bucket CORS rules.
    pub(crate) fn replace_bucket_cors_rules(
        transaction: &Transaction,
        rules: &[StoredCorsRule],
    ) -> SqliteResult<()> {
        transaction.execute("DELETE FROM bucket_cors_rule;", ())?;

        let mut stmt = transaction.prepare_cached(
            "
            INSERT INTO bucket_cors_rule (sequence, id, max_age_seconds)
            VALUES (?1, ?2, ?3);",
        )?;
        for (sequence, rule) in rules.iter().enumerate() {
            let sequence =
                i64::try_from(sequence).map_err(|err| ToSqlConversionFailure(Box::new(err)))?;
            stmt.execute((sequence, rule.id.as_deref(), rule.max_age_seconds))?;
            Self::insert_bucket_cors_rule_values(
                transaction,
                CorsRuleValueTable::AllowedOrigin,
                sequence,
                &rule.allowed_origins,
            )?;
            Self::insert_bucket_cors_rule_values(
                transaction,
                CorsRuleValueTable::AllowedMethod,
                sequence,
                &rule.allowed_methods,
            )?;
            Self::insert_bucket_cors_rule_values(
                transaction,
                CorsRuleValueTable::AllowedHeader,
                sequence,
                &rule.allowed_headers,
            )?;
            Self::insert_bucket_cors_rule_values(
                transaction,
                CorsRuleValueTable::ExposeHeader,
                sequence,
                &rule.expose_headers,
            )?;
        }

        Ok(())
    }

    fn get_bucket_cors_rule_values(
        transaction: &Transaction,
        table: CorsRuleValueTable,
        sequence: i64,
    ) -> SqliteResult<Vec<String>> {
        let mut stmt = transaction.prepare_cached(table.select_sql())?;
        Ok(stmt
            .query_map((sequence,), |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn insert_bucket_cors_rule_values(
        transaction: &Transaction,
        table: CorsRuleValueTable,
        sequence: i64,
        values: &[String],
    ) -> SqliteResult<()> {
        let mut stmt = transaction.prepare_cached(table.insert_sql())?;
        for (position, value) in values.iter().enumerate() {
            let position =
                i64::try_from(position).map_err(|err| ToSqlConversionFailure(Box::new(err)))?;
            stmt.execute((sequence, position, value.as_str()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::sqlite::{Sqlite, SqliteResult, StoredCorsRule};

    fn open_schema() -> SqliteResult<Connection> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch("PRAGMA foreign_keys=true;")?;
        let transaction = connection.transaction()?;
        Sqlite::create_tables(&transaction)?;
        transaction.commit()?;
        Ok(connection)
    }

    fn rules() -> Vec<StoredCorsRule> {
        vec![
            StoredCorsRule {
                id: Some("images".to_string()),
                allowed_origins: vec!["https://app.example.test".to_string()],
                allowed_methods: vec!["GET".to_string(), "HEAD".to_string()],
                allowed_headers: vec!["authorization".to_string(), "x-amz-*".to_string()],
                expose_headers: vec!["ETag".to_string()],
                max_age_seconds: Some(600),
            },
            StoredCorsRule {
                id: None,
                allowed_origins: vec!["*".to_string()],
                allowed_methods: vec!["PUT".to_string()],
                allowed_headers: Vec::new(),
                expose_headers: Vec::new(),
                max_age_seconds: None,
            },
        ]
    }

    #[test]
    fn bucket_cors_rules_replace_and_read_in_rule_order() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;

        Sqlite::replace_bucket_cors_rules(&transaction, &rules())?;
        assert_eq!(Sqlite::get_bucket_cors_rules(&transaction)?, rules());

        let replacement = vec![StoredCorsRule {
            id: Some("replacement".to_string()),
            allowed_origins: vec!["https://new.example.test".to_string()],
            allowed_methods: vec!["POST".to_string()],
            allowed_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age_seconds: Some(1),
        }];
        Sqlite::replace_bucket_cors_rules(&transaction, &replacement)?;
        assert_eq!(Sqlite::get_bucket_cors_rules(&transaction)?, replacement);

        Sqlite::replace_bucket_cors_rules(&transaction, &[])?;
        assert!(Sqlite::get_bucket_cors_rules(&transaction)?.is_empty());

        Ok(())
    }

    #[test]
    fn bucket_cors_value_rows_cascade_with_rules() -> SqliteResult<()> {
        let mut connection = open_schema()?;
        let transaction = connection.transaction()?;

        Sqlite::replace_bucket_cors_rules(&transaction, &rules())?;
        transaction.execute("DELETE FROM bucket_cors_rule;", ())?;

        for table in [
            "bucket_cors_allowed_origin",
            "bucket_cors_allowed_method",
            "bucket_cors_allowed_header",
            "bucket_cors_expose_header",
        ] {
            let count: i64 =
                transaction.query_row(&format!("SELECT COUNT(*) FROM {table};"), (), |row| {
                    row.get(0)
                })?;
            assert_eq!(count, 0, "{table} rows should cascade");
        }

        Ok(())
    }
}
