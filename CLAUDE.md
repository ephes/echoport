# CLAUDE.md - Echoport

## Project

Django-based backup orchestration service that triggers backups/restores via FastDeploy.

## Rules

- **Never commit files in `specs/`** — specs are local-only design documents, not version controlled.
- Follow existing code patterns (centralized validation, model clean() + admin form validation).
- Run `.venv/bin/pytest` to verify changes before committing.
- manage.py is at `src/django/manage.py`, use `.venv/bin/python` to run it.
