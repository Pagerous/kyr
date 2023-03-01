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


async def dispatch_events(events: Iterable[events.Event]):
    for event_ in events:
        if isinstance(event_, service_events.OrganizationUpdated):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Updated '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.ReposRemoved):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Removed "
                f"{len(event_.repo_names)} repos in '{event_.org_name}'",
                fg="yellow",
            )
        elif isinstance(event_, service_events.ReposUpdated):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Updated "
                f"{len(event_.repo_names)} repos in '{event_.org_name}'",
                fg="green",
            )
            await dispatch_events(
                await commands.pull_repo_dependencies(
                    git_host=event_.git_host,
                    org_name=event_.org_name,
                    repo_names=event_.repo_names,
                )
            )
        elif isinstance(event_, service_events.ReposNotFound):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Repos "
                f"{list(event_.repo_names)} in '{event_.org_name}' were not "
                "found",
                fg="yellow",
            )
        elif isinstance(event_, service_events.ReposAccessForbidden):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Access in repos "
                f"{list(event_.repo_names)} in '{event_.org_name}' "
                "is forbidden. Ensure your access is still valid.",
                fg="red",
            )
        elif isinstance(event_, service_events.ReposListAccessForbidden):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Access for repos listing in "
                f"'{event_.org_name}' is forbidden. Ensure your access is "
                "still valid.",
                fg="red",
            )
        elif isinstance(event_, service_events.ReposFileAccessForbidden):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Access for file "
                f"'{event_.file_path}' in repos {list(event_.repo_names)} "
                f"in '{event_.org_name}' is forbidden. Ensure your access is "
                f"still valid.",
                fg="red",
            )
        elif isinstance(event_, service_events.ReposDependenciesUpdated):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Updated dependencies in "
                f"{len(event_.repo_names)} repos in '{event_.org_name}'",
                fg="green",
            )


@click.group()
@click.pass_context
def pull(ctx: click.Context):
    ctx.obj["GIT_HOST"] = GitHub(Config.get("github.token"))


@pull.command()
@coroutine
@click.argument("org-name")
@click.pass_context
async def org(ctx: click.Context, org_name):
    await dispatch_events(
        commands.pull_organization_data(
            git_host=ctx.obj.get("GIT_HOST"),
            org_name=org_name,
        )
    )


@pull.command()
@coroutine
@click.option("-o", "--org-name", type=str)
@click.argument("repo_names", nargs=-1)
@click.pass_context
async def repos(ctx: click.Context, org_name: str, repo_names):
    await dispatch_events(
        await commands.pull_repos_data(
            git_host=ctx.obj.get("GIT_HOST"),
            org_name=org_name,
            repo_names=repo_names,
        )
    )
