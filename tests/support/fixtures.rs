use uuid::Uuid;

pub fn bucket_name(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}
