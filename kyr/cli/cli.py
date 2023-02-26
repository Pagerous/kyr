import click
from kyr.cli.commands.pull import pull


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)


cli.add_command(pull)


if __name__ == "__main__":
    cli()
