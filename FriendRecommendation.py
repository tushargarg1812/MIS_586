#This code give recommendation of friends if the mutual friends is greater
#than or equal to three

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
            reducer=self.reducer_int),
            MRStep(reducer=self.reducer_final)]

    def mapper(self, _, data):
        #This mapper reads the data from the csv file. This mapper
        #outputs friend's bi directional relations.

        friends = data.split("\n")
        for friend in friends:
            names = friend.split(',')
            yield names[0], names[1:]
            yield names[1], names[:-1]

    def reducer_int(self, name_key, name_value):
        #This reducer takes the both friends as key and value. And
        #outputs the result as none as key and friend relations as
        #value.

        b = []
        for name in name_value:
            b.append(name)
        yield None, (name_key, b)

    def reducer_final(self, _, connection_list):
        #This reducer receives the data as null and friends relations.
        #This paper performs the required filtering to generate the final
        #output.

        connection_dict = dict()
        recommend_dict = dict()
        recommend_list = []
        intermediate_list = []

        for connection in connection_list:
            for elements in connection[1]:
                intermediate_list.append(elements)
            connection_dict[connection[0]] = intermediate_list
            intermediate_list =[]

        for key1 in connection_dict:
            for key2 in connection_dict:
                flat_list_1 = [item for sublist in connection_dict[key1] for item in sublist]
                flat_list_2 = [item for sublist in connection_dict[key2] for item in sublist]
                if key2 not in flat_list_1 and key1 != key2:
                    if len(set(flat_list_1) & set(flat_list_2)) > 2:
                        recommend_list.append(key2)
            recommend_dict[key1] = recommend_list
            recommend_list = []
        for key, value in recommend_dict.items():
            print(key,':Connections', value)

if __name__ == '__main__':
    MRMostUsedWord.run()
