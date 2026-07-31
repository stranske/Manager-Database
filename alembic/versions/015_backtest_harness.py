"""Add price_cache and backtest tables for the signal-alpha harness (#1464).

Revision ID: 015
Revises: 014
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sqlite_autoincrement_id = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
    op.create_table(
        "price_cache",
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("price_date", sa.Date(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default=text("'stooq'")),
        sa.Column("close_usd", sa.Numeric(), nullable=True),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("ticker", "price_date", "source"),
    )
    op.create_index("idx_price_cache_ticker_date", "price_cache", ["ticker", "price_date"])

    op.create_table(
        "backtest_runs",
        sa.Column("run_id", sqlite_autoincrement_id, autoincrement=True),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("manager_id", sa.BigInteger(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("entry_lag_days", sa.Integer(), nullable=False, server_default=text("0")),
        sa.Column("holding_period_days", sa.Integer(), nullable=False, server_default=text("91")),
        sa.Column("benchmark_ticker", sa.Text(), nullable=True),
        sa.Column("price_source", sa.Text(), nullable=True),
        sa.Column("periods", sa.Integer(), nullable=False, server_default=text("0")),
        sa.Column("positions", sa.Integer(), nullable=False, server_default=text("0")),
        sa.Column("positions_skipped", sa.Integer(), nullable=False, server_default=text("0")),
        sa.Column("total_return", sa.Float(), nullable=True),
        sa.Column("annualized_return", sa.Float(), nullable=True),
        sa.Column("sharpe", sa.Float(), nullable=True),
        sa.Column("hit_rate", sa.Float(), nullable=True),
        sa.Column("benchmark_total_return", sa.Float(), nullable=True),
        sa.Column("excess_return", sa.Float(), nullable=True),
        sa.Column("params_json", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_backtest_runs")),
    )
    op.create_index(
        "idx_backtest_runs_strategy",
        "backtest_runs",
        ["strategy", "created_at"],
    )

    op.create_table(
        "backtest_results",
        sa.Column("result_id", sqlite_autoincrement_id, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("decision_date", sa.Date(), nullable=False),
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column("exit_date", sa.Date(), nullable=False),
        sa.Column("ticker", sa.Text(), nullable=True),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("entry_price", sa.Float(), nullable=True),
        sa.Column("exit_price", sa.Float(), nullable=True),
        sa.Column("position_return", sa.Float(), nullable=True),
        sa.Column("benchmark_return", sa.Float(), nullable=True),
        sa.Column("excess_return", sa.Float(), nullable=True),
        sa.Column("weight", sa.Float(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=text("'filled'")),
        sa.Column("skip_reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("result_id", name=op.f("pk_backtest_results")),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["backtest_runs.run_id"],
            name=op.f("fk_backtest_results_run"),
            ondelete="CASCADE",
        ),
    )
    op.create_index("idx_backtest_results_run", "backtest_results", ["run_id"])


def downgrade() -> None:
    op.drop_index("idx_backtest_results_run", table_name="backtest_results")
    op.drop_table("backtest_results")
    op.drop_index("idx_backtest_runs_strategy", table_name="backtest_runs")
    op.drop_table("backtest_runs")
    op.drop_index("idx_price_cache_ticker_date", table_name="price_cache")
    op.drop_table("price_cache")
