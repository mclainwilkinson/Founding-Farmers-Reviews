from mpi4py import MPI
from bs4 import BeautifulSoup
import requests
import pandas as pd

# initialize MPI
comm = MPI.COMM_WORLD  
size = comm.Get_size()
rank = comm.Get_rank() 
stat = MPI.Status()

# create dictionary to store date, rating, review_text values
ff_reviews = {'date':[], 'rating':[], 'review':[]}

# 584 pages of reviews = ~11680 total reviews
for n in range(584):
    if n % size == rank:
        if n == 0:
            end = ''
        else:
            end = '?start=' + str(n * 20)

        url = 'https://www.yelp.com/biz/founding-farmers-dc-washington-4' + end

        response = requests.get(url)

        html_soup = BeautifulSoup(response.text,'html.parser') 
        info = html_soup.find_all('div', {'class':'review-content'}) 
        for i in info:
            review_text = i.find('p', {'lang':'en'}).text.strip()
            ff_reviews['review'].append(review_text)
            review_date = i.find('span', {'class':'rating-qualifier'}).text.strip().split('\n')[0]
            ff_reviews['date'].append(review_date)
            rating = float(i.find('img')['alt'].strip()[:3])
            ff_reviews['rating'].append(rating)

# convert dictionary to pandas df
ff = pd.DataFrame.from_dict(ff_reviews)

# combine results at rank 0 node
results = comm.gather(ff, root = 0)

# sort and fix datatypes for rank 0
if rank == 0:
    ff = pd.concat(results)
    ff['date'] = pd.to_datetime(ff['date'])
    ff.to_csv('ffreviews.csv', index=False)
    print(ff.head())
