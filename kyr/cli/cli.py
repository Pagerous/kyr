import click

from kyr.cli.commands.pull import pull
from kyr.cli.commands.show import show


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)


cli.add_command(pull)
cli.add_command(show)


if __name__ == "__main__":
    cli()
