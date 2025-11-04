use std::{
    fs, io,
    path::{Path, PathBuf},
};

use alloy::json_abi::JsonAbi;
use dialoguer::Completion;

use crate::errors::RunicError;

pub fn print_banner(title: &str) {
    println!();
    println!("{title}");
    println!("{}", "=".repeat(title.len()));
}

pub fn load_json_abi(path: &Path) -> Result<JsonAbi, RunicError> {
    if !path.exists() {
        return Err(RunicError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("ABI file `{}` not found", path.display()),
        )));
    }

    let contents = fs::read_to_string(path)?;

    serde_json::from_str(&contents).map_err(|err| {
        RunicError::Abi(format!(
            "Failed to parse ABI `{}`: {err}",
            path.display()
        ))
    })
}

#[derive(Default)]
pub struct SimplePathCompletion;

impl Completion for SimplePathCompletion {
    fn get(&self, input: &str) -> Option<String> {
        let trimmed = input.trim();
        let sep = std::path::MAIN_SEPARATOR;

        let (dir, prefix) = if trimmed.is_empty() {
            (PathBuf::from("."), String::new())
        } else if trimmed.ends_with(sep) || trimmed.ends_with('/') {
            (PathBuf::from(trimmed), String::new())
        } else {
            let path = PathBuf::from(trimmed);
            let prefix = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_string();
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            (parent.to_path_buf(), prefix)
        };

        let entries = fs::read_dir(&dir).ok()?;
        let mut matches: Vec<(String, bool)> = entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy().into_owned();
                if prefix.is_empty() || name.starts_with(&prefix) {
                    let is_dir = entry
                        .file_type()
                        .map(|ft| ft.is_dir())
                        .unwrap_or(false);
                    Some((name, is_dir))
                } else {
                    None
                }
            })
            .collect();

        if matches.is_empty() {
            return None;
        }

        matches.sort_by(|a, b| a.0.cmp(&b.0));
        let (name, is_dir) = matches.first().cloned().unwrap();

        let suggestion = if trimmed.is_empty() {
            name
        } else if trimmed.ends_with(sep) || trimmed.ends_with('/') {
            format!("{trimmed}{name}")
        } else if let Some(pos) = trimmed.rfind([sep, '/']) {
            let (head, _) = trimmed.split_at(pos + 1);
            format!("{head}{name}")
        } else {
            name
        };

        if is_dir
            && !suggestion.ends_with(sep)
            && !suggestion.ends_with('/')
        {
            Some(format!("{suggestion}{sep}"))
        } else {
            Some(suggestion)
        }
    }
}
