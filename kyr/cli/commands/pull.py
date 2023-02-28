import asyncio
from functools import update_wrapper
from typing import Iterable

import click

from kyr.config import Config
from kyr.service import commands
from kyr.service import events
from kyr.service import events as service_events
from kyr.service.pull.host import GitHub


def coroutine(f):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return update_wrapper(wrapper, f)


def dispatch_events(events: Iterable[events.Event]):
    for event_ in events:
        if isinstance(event_, service_events.OrganizationPulled):
            click.secho(
                f"{event_.git_host.upper()}: Pulled '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.OrganizationUpdated):
            click.secho(
                f"{event_.git_host.upper()}: Updated '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.ReposPulled):
            click.secho(
                f"{event_.git_host.upper()}: Pulled {len(event_.repo_names)} "
                f"repos for '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.ReposRemoved):
            click.secho(
                f"{event_.git_host.upper()}: Removed {len(event_.repo_names)} "
                f"repos for '{event_.org_name}'",
                fg="yellow",
            )
        elif isinstance(event_, service_events.ReposUpdated):
            click.secho(
                f"{event_.git_host.upper()}: Updated {len(event_.repo_names)} "
                f"repos for '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.ReposNotFound):
            click.secho(
                f"{event_.git_host.upper()}: Repos {list(event_.repo_names)} "
                f"for '{event_.org_name}' were not found",
                fg="yellow",
            )
        elif isinstance(event_, service_events.ReposAccessForbidden):
            click.secho(
                f"{event_.git_host.upper()}: Access for repos "
                f"{list(event_.repo_names)} for '{event_.org_name}' "
                "is forbidden. Ensure your access is still valid.",
                fg="red",
            )
        elif isinstance(event_, service_events.ReposListAccessForbidden):
            click.secho(
                f"{event_.git_host.upper()}: Access for repos listing for "
                f"'{event_.org_name}' is forbidden. Ensure your access is "
                "still valid.",
                fg="red",
            )


@click.group()
@click.pass_context
def pull(ctx: click.Context):
    ctx.obj["GIT_HOST"] = GitHub(Config.get("github.token"))


@pull.command()
@click.argument("org-name")
@click.pass_context
def org(ctx: click.Context, org_name):
    events = commands.pull_organization_data(
        git_host=ctx.obj.get("GIT_HOST"),
        org_name=org_name,
    )
    dispatch_events(events)


@pull.command()
@coroutine
@click.option("-o", "--org-name", type=str)
@click.argument("repo_names", nargs=-1)
@click.pass_context
async def repos(ctx: click.Context, org_name: str, repo_names):
    dispatch_events(
        await commands.pull_repos_data(
            git_host=ctx.obj.get("GIT_HOST"),
            org_name=org_name,
            repo_names=repo_names,
        )
    )
    dispatch_events(
        await commands.pull_repo_dependencies(
            git_host=ctx.obj.get("GIT_HOST"),
            org_name=org_name,
            repo_names=repo_names,
        )
    )
