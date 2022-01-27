import collections
from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep

# calculate movie ratings sum and sort result by ascending order
class RatingBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
            , MRStep(reducer=self.reducer_sorted_output)

        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sorted_output(self, rating, movies):
        for movie in movies:
            yield movie, rating


if __name__ == '__main__':
    RatingBreakdown.run()
