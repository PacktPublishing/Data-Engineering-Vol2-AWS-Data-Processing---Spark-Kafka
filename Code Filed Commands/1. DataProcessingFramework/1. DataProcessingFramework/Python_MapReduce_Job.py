from mrjob.job import MRJob, MRStep
import csv

class MapReduceJob3aayaam(MRJob):

    def map_key_value(self, key, line):
        record = next(csv.reader([line]))
        key = record[14]
        value = float((record[6].strip()))
        yield (key, value)

    def map_count_by_country(self, key, line):
        yield (key, 1)

    def reduce_count_by_country(self, key, value):
        yield (key, sum(value))

    def reduce_spend_by_country(self, key, value):
        yield(key, sum(value))

    def map_filter_country(self, key, value):
        if key == "India":
            country_key = key
            country_value = value
            yield (country_key, country_value)

    def steps(self):
        return [
            # MRStep(mapper=self.map_key_value, reducer=self.reduce_spend_by_country),
            # MRStep(mapper=self.map_key_value),
            # MRStep(mapper=self.map_count_by_country,reducer=self.reduce_count_by_country)
            MRStep(mapper=self.map_key_value,),
            MRStep(mapper=self.map_filter_country),
        ]

if __name__ == "__main__":
    MapReduceJob3aayaam.run()