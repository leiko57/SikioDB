use crate::error::{Result, SikioError};
pub const CURRENT_SCHEMA_VERSION: u32 = 1;
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}
impl SchemaVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        SchemaVersion {
            major,
            minor,
            patch,
        }
    }
    pub fn current() -> Self {
        SchemaVersion::new(CURRENT_SCHEMA_VERSION, 0, 0)
    }
    pub fn from_u32(version: u32) -> Self {
        SchemaVersion {
            major: (version >> 16) & 0xFFFF,
            minor: (version >> 8) & 0xFF,
            patch: version & 0xFF,
        }
    }
    pub fn to_u32(&self) -> u32 {
        ((self.major & 0xFFFF) << 16) | ((self.minor & 0xFF) << 8) | (self.patch & 0xFF)
    }
    pub fn is_compatible(&self, other: &SchemaVersion) -> bool {
        self.major == other.major
    }
    pub fn needs_migration(&self, target: &SchemaVersion) -> bool {
        self.to_u32() < target.to_u32()
    }
}
impl Default for SchemaVersion {
    fn default() -> Self {
        SchemaVersion::current()
    }
}
pub trait Migration {
    fn version(&self) -> SchemaVersion;
    fn description(&self) -> &str;
    fn up(&self, data: &mut Vec<u8>) -> Result<()>;
    fn down(&self, data: &mut Vec<u8>) -> Result<()>;
}
pub struct MigrationRunner {
    migrations: Vec<Box<dyn Migration>>,
}
impl MigrationRunner {
    pub fn new() -> Self {
        MigrationRunner {
            migrations: Vec::new(),
        }
    }
    pub fn add_migration(&mut self, migration: Box<dyn Migration>) {
        self.migrations.push(migration);
        self.migrations.sort_by_key(|m| m.version().to_u32());
    }
    pub fn run_migrations(
        &self,
        current: &SchemaVersion,
        target: &SchemaVersion,
        data: &mut Vec<u8>,
    ) -> Result<Vec<String>> {
        let mut applied = Vec::new();
        if current.to_u32() < target.to_u32() {
            for migration in &self.migrations {
                let mv = migration.version();
                if mv.to_u32() > current.to_u32() && mv.to_u32() <= target.to_u32() {
                    migration.up(data)?;
                    applied.push(migration.description().to_string());
                }
            }
        } else if current.to_u32() > target.to_u32() {
            for migration in self.migrations.iter().rev() {
                let mv = migration.version();
                if mv.to_u32() <= current.to_u32() && mv.to_u32() > target.to_u32() {
                    migration.down(data)?;
                    applied.push(format!("Rollback: {}", migration.description()));
                }
            }
        }
        Ok(applied)
    }
    pub fn pending_migrations(&self, current: &SchemaVersion) -> Vec<&str> {
        self.migrations
            .iter()
            .filter(|m| m.version().to_u32() > current.to_u32())
            .map(|m| m.description())
            .collect()
    }
}
impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}
pub fn validate_schema_version(stored: u32) -> Result<SchemaVersion> {
    let version = SchemaVersion::from_u32(stored);
    let current = SchemaVersion::current();
    if version.major > current.major {
        return Err(SikioError::Corrupted(format!(
            "Database schema version {} is newer than supported {}",
            stored,
            current.to_u32()
        )));
    }
    Ok(version)
}
