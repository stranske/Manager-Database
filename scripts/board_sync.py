"""Utility to sync GitHub project status from CI."""

import argparse
import subprocess


def update_status(issue_id: str, status: str, project: str = "Manager-Intel") -> None:
    """Update the GitHub Project board status for an Issue."""
    # Build the gh CLI command with provided arguments
    cmd = [
        "gh",
        "project",
        "item-status",
        "--project",
        project,
        "--item",
        issue_id,
        "--status",
        status,
    ]
    # Run the command and bubble up any errors to the caller
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Project board status")
    parser.add_argument("issue_id", help="Issue number to update")
    parser.add_argument(
        "status",
        choices=["Backlog", "In Progress", "Review", "Done"],
        help="Target column on the Project board",
    )
    parser.add_argument(
        "--project",
        default="Manager-Intel",
        help="GitHub project name",
    )
    args = parser.parse_args()
    update_status(args.issue_id, args.status, args.project)
