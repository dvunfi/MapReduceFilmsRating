import collections
from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep

# calculate movie ratings count by stars from 1 to 5
class RatingBreakup(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_movie_ratings_stat)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, rating

    def reducer_movie_ratings_stat(self, key, values):
        yield key, sorted(collections.Counter(list(values)).items())


if __name__ == '__main__':
    RatingBreakup.run()
