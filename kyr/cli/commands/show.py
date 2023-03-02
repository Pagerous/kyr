import operator

import click

from kyr.service.query import DependencyRequirement, get_repos


def query_parser(query: str) -> dict[tuple[str, str], DependencyRequirement]:
    tokens = query.split("&")
    deps = {}
    for token in tokens:
        name, constraint, version = token.split()
        match constraint:
            case "==":
                operator_ = operator.eq
            case "!=":
                operator_ = operator.ne
            case ">=":
                operator_ = operator.ge
            case ">":
                operator_ = operator.gt
            case "<=":
                operator_ = operator.le
            case "<":
                operator_ = operator.lt
            case _:
                raise RuntimeError(f"unsupported constraint '{constraint}'")
        deps[("python", name)] = DependencyRequirement(
            "python", name, version, operator_
        )
    return deps


@click.group()
def show():
    pass


@show.command()
@click.argument("query", type=str)
def deps(query):
    repos = get_repos(dependency_requirements=query_parser(query))
    for repo in repos:
        click.echo(f"\033]8;;{repo.html_url}\033\\{repo.name}\033]8;;\033\\")
