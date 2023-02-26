import asyncio
from functools import update_wrapper

import click

from kyr.config import Config
from kyr.service import commands


def coroutine(f):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return update_wrapper(wrapper, f)


@click.group()
@click.option("-g", "--git-host", type=click.Choice(["github"]), default="github")
@click.pass_context
def pull(ctx: click.Context, git_host):
    ctx.obj["GIT_HOST"] = git_host


@pull.command()
@click.argument("org-name")
@click.pass_context
def org(ctx: click.Context, org_name):
    commands.pull_organization_data(
        git_host=ctx.obj.get("GIT_HOST"),
        org_name=org_name,
        github_token=Config.get("github.token")
    )


@pull.command()
@coroutine
@click.option("-o", "--org-name", type=str)
@click.argument("repo_names", nargs=-1)
@click.pass_context
async def repos(ctx: click.Context, org_name: str, repo_names):
    await commands.pull_repos_data(
        git_host=ctx.obj.get("GIT_HOST"),
        org_name=org_name,
        repo_names=repo_names,
        github_token=Config.get("github.token")
    )
