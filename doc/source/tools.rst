=======
Tooling
=======


Along our development we added tools to help one to ship faster. It's important
to know how to use them but it's critical to know they actually exist in the
first place.


Env
===

`source .env` will supercharge your shell with a few helpers you may like:

- Add the current directory to your `PYTHONPATH`
- Add `./tools/cli` to your `PATH` (so you can run them directly, like `kp-shub`)
- Unset datadog api keys to avoid sending local metrics by accident

- Register the alias `scrape` which replaces `scrapy crawl` with sensible defaults
- Register the alias `stream` which output the json items in
  `/tmp/scrapy-stream.jl`. Tip: use this command and in another terminal run
  `tail -f /tmp/scrapy-stream.jlL jq "."` to have a more readable output. This
  is actually what `tsream` does, another alias, using `tmux`.
- Register `prod_scrape` which will output items in the same layout the ETL is
  expecting them (to be deprecated)
- Register the alias `doctor` that checks mandatory attributes of all spiders


Git hooks
=========

Those scripts aim at making our workflow consistent across developers (like
commit messages) and minimizing human mistakes (like linting). It's also the
opportunity to improve our delivery pipeline and automate simple, repetitive
things.

As explained in the setup section, they are regular scripts wrapped up by
git-hooks_ into the `./githooks/{hook name}` folder.

Once ran `git hooks install` they will run automatically along our `Github
workflow`_ so it's good to be aware of what's in there. Running `git hooks`
alone will list those scritps (if not, you are welcome to update the doc :) ):

**Pre push**

- Requires user confirmation before opushing to a remote branch. Currentyl
configured to protect master.

**Commit Message**

- Will ask dev if we should skip CI tests in case the commit message include `WIP`.

**Post Checkout**

- Delete Python garbage like `pyc` files, cache directories, package build, ...

**Post commit**

- Re-inspect the repository and rebuild project tags.

**Pre commit**

- Check style against flake8 since it failure will break the build anyway.
- Check imports are following isort_ rules



.. _git-hooks: https://github.com/git-hooks/git-hooks
.. _`github workflow`: https://guides.github.com/introduction/flow/index.html<Paste>
.. _isort: http://timothycrosley.github.io/isort/
