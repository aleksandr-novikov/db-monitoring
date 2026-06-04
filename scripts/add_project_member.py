"""CLI to add a user to a project as a member (#172).

Usage:
    python -m scripts.add_project_member \
        --owner demo@dbmonitor.app \
        --slug retail-postgres \
        --email demo2@dbmonitor.app \
        --role editor

Resolves the project by (owner-email, slug) — слаги уникальны per-owner,
поэтому нужны оба. Если юзер уже состоит в проекте — UPSERT
обновит его role.

Owner row создаётся автоматически в ``create_project``; этот скрипт —
только для editor / viewer membership.
"""

from __future__ import annotations

import argparse
import sys

from app.metrics_storage import (
    InvalidMemberRole,
    add_project_member,
    get_project_by_slug,
    get_user_by_email,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--owner", required=True,
        help="Email владельца проекта (чтобы резолвить slug per-owner)",
    )
    parser.add_argument(
        "--slug", required=True,
        help="Slug проекта в namespace владельца",
    )
    parser.add_argument(
        "--email", required=True,
        help="Email пользователя, которого добавляем как члена",
    )
    parser.add_argument(
        "--role", default="editor", choices=("editor", "viewer"),
        help="Роль (owner создаётся через create_project, не здесь)",
    )
    args = parser.parse_args()

    owner = get_user_by_email(args.owner.strip().lower())
    if owner is None:
        print(f"FATAL: owner user not found: {args.owner}", file=sys.stderr)
        return 2
    project = get_project_by_slug(owner["id"], args.slug)
    if project is None:
        print(
            f"FATAL: project '{args.slug}' not found under owner {args.owner}",
            file=sys.stderr,
        )
        return 2
    member = get_user_by_email(args.email.strip().lower())
    if member is None:
        print(f"FATAL: target user not found: {args.email}", file=sys.stderr)
        return 2

    try:
        row = add_project_member(project["id"], member["id"], role=args.role)
    except InvalidMemberRole as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2

    print(
        f"Added {args.email} to project '{args.slug}' as {row['role']} "
        f"(project_id={project['id']}, user_id={member['id']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
