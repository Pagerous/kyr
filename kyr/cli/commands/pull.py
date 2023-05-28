import asyncio
import logging
from functools import partial, update_wrapper
from typing import Iterable

import click

from kyr.config import Config
from kyr.service import commands
from kyr.service import events as service_events
from kyr.service.pull.host import GitHub, GitHubTokenManager, GitHubToken
from kyr.service.pull import filters

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()


def coroutine(f):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return update_wrapper(wrapper, f)


async def dispatch_events(events: Iterable[service_events.Event]):
    for event_ in events:
        if isinstance(event_, service_events.OrganizationUpdated):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Updated '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.OrganizationPullFailed):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Organization pull failed. "
                f"Entire pull operation was canceled. Reason: {event_.reason}. ",
                fg="red",
            )
        elif isinstance(event_, service_events.ReposUpdated):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Updated "
                f"{len(event_.repo_names)} repos in '{event_.org_name}'",
                fg="green",
            )
        elif isinstance(event_, service_events.ReposListingPullFailed):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Repos listing operation failed. "
                f"Entire pull operation was canceled. Reason: {event_.reason}. ",
                fg="red",
            )
        elif isinstance(event_, service_events.RepoPullFailed):
            click.secho(
                f"{event_.git_host.NAME.upper()}: Repo '{event_.repo_name}' was not pull due to error. "
                f"Reason: {event_.reason}. ",
                fg="red",
            )
        elif isinstance(event_, service_events.RepoFilePullFailed):
            click.secho(
                f"{event_.git_host.NAME.upper()}: File '{event_.file_path}' from repo '{event_.repo_name}' "
                f"was not pull due to error. Reason: {event_.reason}. ",
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
    ctx.obj["GIT_HOST"] = GitHub(
        logger=logger,
        token_manager=GitHubTokenManager(tokens=[GitHubToken(Config.get("github.token"), last_expired=None)])
    )


@pull.command()
@coroutine
@click.argument("org-name")
@click.pass_context
async def org(ctx: click.Context, org_name):
    await dispatch_events(
        commands.pull_organization(
            git_host=ctx.obj.get("GIT_HOST"),
            org_name=org_name,
        )
    )


@pull.command()
@coroutine
@click.option("-o", "--org-name", type=str)
@click.pass_context
async def repos(ctx: click.Context, org_name: str):
    with click.progressbar(show_percent=True, length=1) as bar:
        events_ = await commands.pull_repos(
            git_host=ctx.obj.get("GIT_HOST"),
            org_name=org_name,
            filter_=filters.RepoFilter(filters.StartsWith("limepkg")),
            n_repos_determined_callback=partial(_set_progress_bar_length, bar=bar),
            repo_fetched_callback=partial(_repo_fetched, bar=bar),
        )
        await dispatch_events(events_)


def _set_progress_bar_length(n_repos, bar):
    bar.length = n_repos


def _repo_fetched(bar):
    bar.update(1)
