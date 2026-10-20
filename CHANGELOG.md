# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-10-18

### Added
- Scope-aware sliding-window rate limiting across per-chat, per-group and global scopes
- Redis (Lua), SQLite and in-memory backends
- Server retry_after learning with a strict-priority waiter queue
- aiogram adapter and a paced, resumable broadcast CLI
