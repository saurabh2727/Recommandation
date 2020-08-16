
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
from math import sqrt

from itertools import combinations




#cols = 'marketplace customer_id	review_id product_id product_parent product_title product_category star_rating helpful_votes total_votes vine verified_purchase review_headline review_body review_date'.split(' ')

def parse_ReviewDate(date_str):
    if date_str == '':
        return -1

    rd = datetime.datetime.strptime(date_str, '%m/%d/%Y')
    years = (datetime.datetime.now() - rd).days / 365

    return years
    
    
    
def parse_Rating(Rating_str):
    return float(Rating_str.strip().strip('$')) 
    
       
def median(list_of_ratings):

    return sorted(list_of_ratings)[len(list_of_ratings)/2]             
    

class AmazonSimilarityReview(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_customer),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity)]

    def mapper_parse_input(self, key, line):
        # Outputs customer id => (product id, star_rating)
        (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, 
        total_votes, vine, verified_purchase, review_headline, review_body, review_date) = line.split('\t')
        if (customer_id != 'customer_id'):  # Skip first line
            yield  customer_id, (product_id, float(star_rating))

    def reducer_ratings_by_customer(self, customer_id, itemRatings):
        #Group (item, rating) pairs by customer id

        ratings = []
        for product_id, star_rating in itemRatings:
            ratings.append((product_id, star_rating))

        yield customer_id, ratings

    def mapper_create_item_pairs(self, customer_id, itemRatings):
        # Find every pair of item each customer has rated, and emit
        # each pair with its associated ratings

        # "combinations" finds every possible pair from the list of items
        # this customer rated.
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            product_id1 = itemRating1[0]
            rating1 = itemRating1[1]
            product_id2 = itemRating2[0]
            rating2 = itemRating2[1]

            # Produce both orders so sims are bi-directional
            yield (product_id1, product_id2), (rating1, rating2)
            yield (product_id2, product_id1), (rating2, rating1)


    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarity(self, productPair, ratingPairs):
        # Compute the similarity score between the ratings vectors
        # for each product pair viewed by multiple people

        # Output product pair => score, number of co-ratings

        score, numPairs = self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (numPairs > 2 and score > 0.95):
            yield productPair, (score, numPairs)

    


if __name__ == '__main__':
    AmazonSimilarityReview.run()
