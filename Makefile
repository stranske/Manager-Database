.PHONY: db-migrate db-seed

db-migrate:
	python -m alembic upgrade head

db-seed:
	python scripts/seed_managers.py
