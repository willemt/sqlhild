"""Optimizers.

Attempts to convert query plan iterators into more efficient iterators.

TODO:
    * Merge two filters into one filter if they have the same source table
"""

import collections

from . import iterator


class TeeRemover(object):
    """
    Remove iterators that use tee when it isn't required, ie. there is only
    one source.
    """

    def process(self, plan):
        tee_count = collections.defaultdict(int)
        tee_direction = collections.defaultdict(list)

        self._process(plan, tee_count, tee_direction)

        for tee, count in tee_count.items():
            if count == 1:
                assert(len(tee_direction[tee]) == 1)
                assert(len(tee.sources) == 1)
                tee_direction[tee][0].replace_source(tee, tee.sources[0])

        return plan

    def _process(self, plan, tee_count, tee_direction):
        for source in getattr(plan, 'sources', []):
            if isinstance(source, iterator.Tee):
                if not [s_ for s_ in source.sources if isinstance(s_, iterator.Tee)]:
                    tee_count[source] += 1
                    tee_direction[source].append(plan)
            self._process(source, tee_count, tee_direction)
