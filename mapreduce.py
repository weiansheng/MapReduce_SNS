from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations

class Social(MRJob):

    def mapper_friend_list(self, _, line):
        '''
        Get fridends list, return a list of every'users friends
        '''
        user_1, user_2 = [int(item) for item in line.split()]
        yield (user_1, user_2)
        yield (user_2, user_1)

        #2nd method
        # num = [int(item) for item in line.split()]
        # yield num[0], num[1]
        # yield num[1], num[0]

        #3nd method, string format
        #yield line[0], line[2]
        #yield line[2], line[0]

    def reducer_friend_list(self, user, friends):
        yield (user, [friend for friend in friends])

    def mapper_mutual_friend(self, user, friend):
        '''
        Get Mutual Friend Counts
        '''
        for pair in combinations(friend,2):
            yield sorted(pair), 1
            yield sorted([pair[0],user]), 0
            yield sorted([pair[1],user]), 0

        #2nd method
        # for i, friend_1 in enumerate(friend):
        #     for j in xrange (i):
        #         yield sorted([friend_1, friend[j]]), 1
        #         yield sorted([friend_1, user]), 0
        #         yield sorted([friend[j], user]), 0

    def reducer_mutual_friend(self, pair, count):
        #print pair, count # count is generator, reducer auto aggregate
        lst = list(count)
        #print lst
        if 0 not in lst:
        #if sum(lst) == len(lst):
            yield (pair, sum(lst))

        #2nd method
        # sum_count = 0
        # for coun in count:
        #     sum_count += coun
        #     if coun == 0:
        #         return
        # yield (pair, sum_count)

    def mapper_friend_suggestion(self, pair, count):
        '''
        Get Friend Suggestions
        '''
        yield pair[0], (pair[1], count)
        yield pair[1], (pair[0], count)

    def reducer_friend_suggestion(self, user, friend_count):
        #print user, friend_count
        max_friend = max(friend_count, key=lambda x: x[1])[0]
        # friend_count is generator, max can take generator
        yield (user,max_friend)


    def steps(self):
        return [
            MRStep(mapper=self.mapper_friend_list,
                   reducer=self.reducer_friend_list),
            MRStep(mapper=self.mapper_mutual_friend,
                   reducer=self.reducer_mutual_friend),
            MRStep(mapper=self.mapper_friend_suggestion,
                   reducer=self.reducer_friend_suggestion)
        ]

if __name__ == '__main__':
    print Social.run()
