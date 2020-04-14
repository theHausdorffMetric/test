#! /usr/bin/env python
# -*- coding: utf-8 -*-

from kp_scrapers.spiders.bases.persist import PersistSpider


class DivideAndConquerMixin(PersistSpider):
    def divide_work(self, universe: list, slice_size: int) -> list:
        """Produce a subset of items to process based on previous run."""
        # check where we left the last time
        last_slice = self.persisted_data.get('slice')
        if not last_slice or last_slice > len(universe):
            # nothing found, start from scratch
            # or last run processed the last slice, restart from scratch also
            last_slice = 0

        new_slice = last_slice + slice_size

        # persist state for next run
        self.persisted_data['slice'] = new_slice
        self.persisted_data.save()

        return universe[slice(last_slice, new_slice)]
