# Echoport justfile

# Default recipe - show available commands
default:
    @just --list

# Start development server
dev:
    cd src/django && uv run python manage.py runserver 0.0.0.0:9000

# Start dev server on specific port
dev-port port="8001":
    cd src/django && uv run python manage.py runserver {{port}}

# Run tests
test *args:
    uv run pytest {{args}}

# Run tests with coverage
test-cov:
    uv run pytest --cov=backups --cov-report=html

# Run migrations
migrate:
    cd src/django && uv run python manage.py migrate

# Create new migrations
makemigrations:
    cd src/django && uv run python manage.py makemigrations

# Create development data
devdata:
    cd src/django && uv run python manage.py create_devdata

# Collect static files
collectstatic:
    cd src/django && uv run python manage.py collectstatic --noinput

# Run a backup for a target
backup target:
    cd src/django && uv run python manage.py backup {{target}}

# Open Django shell
shell:
    cd src/django && uv run python manage.py shell

# Run type checking
typecheck:
    uv run mypy src/

# Sync dependencies
sync:
    uv sync

# Reset database (delete and recreate)
reset-db:
    rm -f src/django/db.sqlite3
    just migrate
    just devdata

# Deploy to macmini via ops-control
deploy:
    cd ~/projects/ops-control && just deploy-one echoport
