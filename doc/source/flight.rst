============
Flight rules
============

**Work In Progress - Please contribute**

`Inspired by git-flight-rules. <https://github.com/k88hudson/git-flight-rules>`_

A `guide for astronauts`_ about what to do when things go wrong.


      *Flight Rules* are the hard-earned body of knowledge recorded in manuals that
      list, step-by-step, what to do if X occurs, and why. Essentially, they are
      extremely detailed, scenario-specific standard operating procedures. [...]

      NASA has been capturing our missteps, disasters and solutions since the early
      1960s, when Mercury-era ground teams first started gathering "lessons
      learned" into a compendium that now lists thousands of problematic
      situations, from engine failure to busted hatch handles to computer glitches,
      and their solutions.


-- Chris Hadfield, *An Astronaut's Guide to Life*.


Release a news source
---------------------

Assuming you developed a new spider to scrape a new source, and a new loader to
interpret the raw data and store it into database.

- [X] Validate green PRs and merge them into master
- [X] Confirm that it was deployed by CI on Scrapinghub production env
- [X] Schedule the spider: `./tools/cli/shub batch-schedule --config ./scheduling/{ category }.yml` --spider { spider }

You can now confirm that we scrape data and store it. Time to configure the loading of this raw information.



.. _`guide for astronauts`: https://www.jsc.nasa.gov/news/columbia/fr_generic.pdf
