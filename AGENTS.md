# AGENTS.md - Echoport

## Rules for all agents

- **Never commit files in `specs/`** — specs are local-only design documents, not version controlled.
- Follow existing code patterns (centralized validation, model clean() + admin form validation).
- Run `.venv/bin/pytest` to verify changes before committing.
